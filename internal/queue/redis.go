// Package queue - Redis implementation.
//
// # DATA MODEL
//
// Three kinds of key, all under a configurable prefix:
//
//	{prefix}:job:{id}   HASH    every field of a Job
//	{prefix}:pending    ZSET    claimable job IDs, score = unix ms it becomes ready
//	{prefix}:inflight   ZSET    claimed job IDs,   score = unix ms the claim expires
//	{prefix}:dead       ZSET    dead-lettered IDs, score = unix ms it died
//
// # WHY SORTED SETS RATHER THAN STREAMS
//
// Redis Streams with consumer groups are the more obvious choice: the
// pending-entries list and XAUTOCLAIM give redelivery almost for free. Two
// things in our contract tip the balance the other way.
//
// Retry backoff. The Queue contract says a nacked job becomes claimable
// again no sooner than NotBefore. A ZSET scored by ready-time expresses that
// directly - ZRANGEBYSCORE with a ceiling of "now" returns exactly the jobs
// that are due. Streams have no notion of a delayed entry; you would need a
// second structure anyway, and XAUTOCLAIM's min-idle-time conflates "the
// worker died" with "this is scheduled for later".
//
// Opaque payloads. Mutating a job inside a Lua script means either parsing
// it with cjson - which silently reorders keys and truncates int64 to a
// double, corrupting caller payloads - or keeping fields separate. A HASH
// keeps Payload a byte field that Lua never looks at.
//
// The cost is that Claim polls rather than blocking, since no Redis command
// blocks on "lowest score, but only if it is due". PollInterval trades claim
// latency against Redis load.
//
// # ATOMICITY
//
// Every operation touches at least two keys: the index ZSET and the job
// HASH. A pipeline would not help - pipelines batch round trips, they are
// not transactions, and a crash between two pipelined commands leaves a job
// indexed as pending with a state of claimed. Each operation is therefore a
// Lua script, which Redis runs to completion without interleaving.
//
// LIMITATION: the scripts build job keys from a prefix passed in ARGV, so
// Redis Cluster cannot verify every key lands in one slot. This targets a
// single Redis instance. Cluster would need hash-tagged keys, e.g.
// {goqueue}:pending.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ Queue = (*RedisQueue)(nil)

// Defaults for RedisQueue.
const (
	// DefaultClaimTTL is how long a claim is honoured before the reaper may
	// hand the job to another worker. It must exceed the worker pool's
	// JobTimeout, or healthy long jobs get stolen mid-execution.
	DefaultClaimTTL = 60 * time.Second

	// DefaultPollInterval is how long Claim sleeps between scans when the
	// queue is empty.
	DefaultPollInterval = 100 * time.Millisecond

	// DefaultSuccessTTL is how long a succeeded job's hash is kept before
	// Redis expires it. Without expiry, every job ever run would accumulate
	// in memory forever.
	DefaultSuccessTTL = time.Hour
)

// RedisQueue is the Redis implementation of Queue.
type RedisQueue struct {
	rdb    *redis.Client
	prefix string

	claimTTL     time.Duration
	pollInterval time.Duration
	successTTL   time.Duration
}

// Option configures a RedisQueue.
type Option func(*RedisQueue)

// WithClaimTTL sets how long a claim survives before the reaper may take it.
func WithClaimTTL(d time.Duration) Option {
	return func(q *RedisQueue) { q.claimTTL = d }
}

// WithPollInterval sets how long Claim waits between scans.
func WithPollInterval(d time.Duration) Option {
	return func(q *RedisQueue) { q.pollInterval = d }
}

// WithSuccessTTL sets how long succeeded jobs are retained.
func WithSuccessTTL(d time.Duration) Option {
	return func(q *RedisQueue) { q.successTTL = d }
}

// New returns a RedisQueue backed by the given client. rdb must be non-nil.
//
// It pings Redis so that a misconfigured deployment fails at startup rather
// than on the first job.
func New(ctx context.Context, rdb *redis.Client, prefix string, opts ...Option) (*RedisQueue, error) {
	if prefix == "" {
		prefix = "goqueue"
	}
	q := &RedisQueue{
		rdb:          rdb,
		prefix:       prefix,
		claimTTL:     DefaultClaimTTL,
		pollInterval: DefaultPollInterval,
		successTTL:   DefaultSuccessTTL,
	}
	for _, opt := range opts {
		opt(q)
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}
	return q, nil
}

// NewWithRetry is New, but keeps retrying the initial connection until it
// succeeds or within elapses.
//
// This exists because fail-fast and cold-start are in tension. Failing fast
// is right for a misconfigured address - you want a loud error, not a
// process that idles looking healthy. But on a fresh Kubernetes rollout the
// scheduler and workers routinely start before Redis has a DNS record, and
// exiting there produces a burst of CrashLoopBackOff restarts before the
// deployment settles. A bounded retry tells the two cases apart: transient
// unreadiness is absorbed, a genuinely wrong address still fails.
func NewWithRetry(ctx context.Context, rdb *redis.Client, prefix string, within time.Duration, opts ...Option) (*RedisQueue, error) {
	const maxBackoff = 2 * time.Second
	deadline := time.Now().Add(within)
	backoff := 100 * time.Millisecond

	for attempt := 1; ; attempt++ {
		q, err := New(ctx, rdb, prefix, opts...)
		if err == nil {
			return q, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("after %d attempts over %v: %w", attempt, within, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (q *RedisQueue) pendingKey() string  { return q.prefix + ":pending" }
func (q *RedisQueue) inflightKey() string { return q.prefix + ":inflight" }
func (q *RedisQueue) deadKey() string     { return q.prefix + ":dead" }
func (q *RedisQueue) jobPrefix() string   { return q.prefix + ":job:" }
func (q *RedisQueue) jobKey(id string) string {
	return q.jobPrefix() + id
}

func millis(t time.Time) int64 { return t.UnixMilli() }

// ---------------------------------------------------------------------------
// Lua scripts
// ---------------------------------------------------------------------------

// enqueueScript refuses to overwrite an existing job, so a duplicate ID is
// reported rather than silently replacing live work.
//
//	KEYS[1] job hash   KEYS[2] pending zset
//	ARGV[1] job id     ARGV[2] ready-at ms   ARGV[3..] field/value pairs
var enqueueScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 3))
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 1
`)

// claimScript moves one due job from pending to inflight and stamps it.
//
//	KEYS[1] pending    KEYS[2] inflight
//	ARGV[1] now ms     ARGV[2] claim deadline ms
//	ARGV[3] worker id  ARGV[4] claimed-at RFC3339   ARGV[5] job key prefix
var claimScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #ids == 0 then
  return false
end
local id = ids[1]
local jobKey = ARGV[5] .. id
redis.call('ZREM', KEYS[1], id)
if redis.call('EXISTS', jobKey) == 0 then
  -- Index entry with no hash: the job expired out from under it. Dropping
  -- the entry (done by the ZREM above) is the repair.
  return false
end
redis.call('ZADD', KEYS[2], ARGV[2], id)
redis.call('HINCRBY', jobKey, 'attempts', 1)
redis.call('HSET', jobKey, 'state', 'claimed', 'claimed_by', ARGV[3], 'claimed_at', ARGV[4])
redis.call('HDEL', jobKey, 'not_before')
return redis.call('HGETALL', jobKey)
`)

// ackScript marks a claimed job succeeded and sets it to expire.
//
//	KEYS[1] inflight
//	ARGV[1] job id  ARGV[2] job key prefix  ARGV[3] success TTL seconds
var ackScript = redis.NewScript(`
local jobKey = ARGV[2] .. ARGV[1]
if redis.call('EXISTS', jobKey) == 0 then
  return -1
end
if redis.call('HGET', jobKey, 'state') ~= 'claimed' then
  return -2
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', jobKey, 'state', 'succeeded')
redis.call('HDEL', jobKey, 'claimed_by', 'claimed_at')
if tonumber(ARGV[3]) > 0 then
  redis.call('EXPIRE', jobKey, ARGV[3])
end
return 1
`)

// nackScript either reschedules the job or dead-letters it. The decision
// lives here, next to the attempts counter it depends on, so it is made
// atomically with the state change.
//
//	KEYS[1] pending   KEYS[2] inflight   KEYS[3] dead
//	ARGV[1] job id    ARGV[2] job key prefix   ARGV[3] ready-at ms
//	ARGV[4] not-before RFC3339 or ""           ARGV[5] last error or ""
//	ARGV[6] now ms
var nackScript = redis.NewScript(`
local jobKey = ARGV[2] .. ARGV[1]
if redis.call('EXISTS', jobKey) == 0 then
  return -1
end
if redis.call('HGET', jobKey, 'state') ~= 'claimed' then
  return -2
end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', jobKey, 'claimed_by', 'claimed_at')
if ARGV[5] ~= '' then
  redis.call('HSET', jobKey, 'last_error', ARGV[5])
else
  redis.call('HDEL', jobKey, 'last_error')
end

local attempts = tonumber(redis.call('HGET', jobKey, 'attempts')) or 0
local maxAttempts = tonumber(redis.call('HGET', jobKey, 'max_attempts')) or 1
if attempts >= maxAttempts then
  redis.call('HSET', jobKey, 'state', 'dead')
  redis.call('HDEL', jobKey, 'not_before')
  redis.call('ZADD', KEYS[3], ARGV[6], ARGV[1])
  return 2
end

redis.call('HSET', jobKey, 'state', 'pending')
if ARGV[4] ~= '' then
  redis.call('HSET', jobKey, 'not_before', ARGV[4])
else
  redis.call('HDEL', jobKey, 'not_before')
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
`)

// reclaimScript returns jobs whose claims expired to the pending set.
//
// This is what makes "no job is lost if a worker dies" true: a worker that
// is OOM-killed mid-job never acks, its claim ages past the deadline, and
// this hands the job to someone else. Without it the job is stranded in
// inflight forever.
//
//	KEYS[1] pending  KEYS[2] inflight
//	ARGV[1] now ms   ARGV[2] job key prefix   ARGV[3] max to reclaim
var reclaimScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
local n = 0
for _, id in ipairs(ids) do
  local jobKey = ARGV[2] .. id
  redis.call('ZREM', KEYS[2], id)
  if redis.call('EXISTS', jobKey) == 1 then
    redis.call('HSET', jobKey, 'state', 'pending')
    redis.call('HDEL', jobKey, 'claimed_by', 'claimed_at')
    redis.call('ZADD', KEYS[1], ARGV[1], id)
    n = n + 1
  end
end
return n
`)

// ---------------------------------------------------------------------------
// Queue implementation
// ---------------------------------------------------------------------------

// Enqueue implements Queue.
func (q *RedisQueue) Enqueue(ctx context.Context, job *Job) error {
	applyEnqueueDefaults(job)
	if job.Type == "" {
		return errors.New("job type is required")
	}

	readyAt := time.Now().UTC()
	if job.NotBefore != nil {
		readyAt = *job.NotBefore
	}

	argv := []any{job.ID, millis(readyAt)}
	argv = append(argv, job.toHash()...)

	n, err := enqueueScript.Run(ctx, q.rdb,
		[]string{q.jobKey(job.ID), q.pendingKey()}, argv...).Int64()
	if err != nil {
		return fmt.Errorf("enqueue %s: %w", job.ID, err)
	}
	if n == 0 {
		return fmt.Errorf("job %s already exists", job.ID)
	}
	return nil
}

// Claim implements Queue.
func (q *RedisQueue) Claim(ctx context.Context, workerID string, block time.Duration) (*Job, error) {
	deadline := time.Now().Add(block)
	for {
		job, err := q.claimOnce(ctx, workerID)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
		if block <= 0 || !time.Now().Before(deadline) {
			return nil, ErrNoJobAvailable
		}
		// Sleep, but stay responsive to cancellation - a worker shutting
		// down should not sit here for a full poll interval.
		wait := min(q.pollInterval, time.Until(deadline))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (q *RedisQueue) claimOnce(ctx context.Context, workerID string) (*Job, error) {
	now := time.Now().UTC()
	res, err := claimScript.Run(ctx, q.rdb,
		[]string{q.pendingKey(), q.inflightKey()},
		millis(now),
		millis(now.Add(q.claimTTL)),
		workerID,
		now.Format(time.RFC3339Nano),
		q.jobPrefix(),
	).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil // the script returned false: nothing due
	}
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}

	fields, err := flatToMap(res)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return jobFromHash(fields)
}

// Ack implements Queue.
func (q *RedisQueue) Ack(ctx context.Context, jobID string) error {
	code, err := ackScript.Run(ctx, q.rdb,
		[]string{q.inflightKey()},
		jobID, q.jobPrefix(), int64(q.successTTL.Seconds()),
	).Int64()
	if err != nil {
		return fmt.Errorf("ack %s: %w", jobID, err)
	}
	return codeToError(code)
}

// Nack implements Queue.
func (q *RedisQueue) Nack(ctx context.Context, jobID string, reason error, retryAfter time.Duration) error {
	now := time.Now().UTC()
	readyAt := now
	notBefore := ""
	if retryAfter > 0 {
		readyAt = now.Add(retryAfter)
		notBefore = readyAt.Format(time.RFC3339Nano)
	}
	lastErr := ""
	if reason != nil {
		lastErr = reason.Error()
	}

	code, err := nackScript.Run(ctx, q.rdb,
		[]string{q.pendingKey(), q.inflightKey(), q.deadKey()},
		jobID, q.jobPrefix(), millis(readyAt), notBefore, lastErr, millis(now),
	).Int64()
	if err != nil {
		return fmt.Errorf("nack %s: %w", jobID, err)
	}
	return codeToError(code)
}

// Status implements Queue.
func (q *RedisQueue) Status(ctx context.Context, jobID string) (State, error) {
	state, err := q.rdb.HGet(ctx, q.jobKey(jobID), "state").Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrJobNotFound
	}
	if err != nil {
		return "", fmt.Errorf("status %s: %w", jobID, err)
	}
	return State(state), nil
}

// Get implements Queue.
func (q *RedisQueue) Get(ctx context.Context, jobID string) (*Job, error) {
	fields, err := q.rdb.HGetAll(ctx, q.jobKey(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", jobID, err)
	}
	// HGETALL on a missing key returns an empty map, not an error.
	if len(fields) == 0 {
		return nil, ErrJobNotFound
	}
	return jobFromHash(fields)
}

// Depth counts jobs in the non-terminal states plus the dead-letter set.
//
// Succeeded jobs are not counted: they expire after SuccessTTL, so any count
// would be an artefact of the retention window rather than a useful signal.
// Use the jobs_processed_total counter for throughput.
func (q *RedisQueue) Depth(ctx context.Context) (map[State]int, error) {
	pipe := q.rdb.Pipeline()
	pending := pipe.ZCard(ctx, q.pendingKey())
	inflight := pipe.ZCard(ctx, q.inflightKey())
	dead := pipe.ZCard(ctx, q.deadKey())
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("depth: %w", err)
	}
	return map[State]int{
		StatePending: int(pending.Val()),
		StateClaimed: int(inflight.Val()),
		StateDead:    int(dead.Val()),
	}, nil
}

// ReclaimExpired returns jobs whose claims have expired to the pending set
// and reports how many were recovered.
//
// Run it periodically from every worker; it is safe to run concurrently
// because the script is atomic. It is not part of the Queue interface:
// MemoryQueue is single-process and has no equivalent failure to repair.
func (q *RedisQueue) ReclaimExpired(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	n, err := reclaimScript.Run(ctx, q.rdb,
		[]string{q.pendingKey(), q.inflightKey()},
		millis(time.Now().UTC()), q.jobPrefix(), limit,
	).Int64()
	if err != nil {
		return 0, fmt.Errorf("reclaim: %w", err)
	}
	return int(n), nil
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

// codeToError maps the shared return codes used by the mutation scripts.
func codeToError(code int64) error {
	switch code {
	case -1:
		return ErrJobNotFound
	case -2:
		return ErrNotClaimed
	default:
		return nil
	}
}

// toHash flattens a Job into HSET field/value arguments. Payload is passed
// through as raw bytes and never parsed.
func (j *Job) toHash() []any {
	out := []any{
		"id", j.ID,
		"type", j.Type,
		"state", string(j.State),
		"attempts", j.Attempts,
		"max_attempts", j.MaxAttempts,
		"created_at", j.CreatedAt.Format(time.RFC3339Nano),
	}
	if len(j.Payload) > 0 {
		out = append(out, "payload", []byte(j.Payload))
	}
	if j.LastError != "" {
		out = append(out, "last_error", j.LastError)
	}
	if j.ClaimedBy != "" {
		out = append(out, "claimed_by", j.ClaimedBy)
	}
	if j.ClaimedAt != nil {
		out = append(out, "claimed_at", j.ClaimedAt.Format(time.RFC3339Nano))
	}
	if j.NotBefore != nil {
		out = append(out, "not_before", j.NotBefore.Format(time.RFC3339Nano))
	}
	return out
}

func jobFromHash(f map[string]string) (*Job, error) {
	job := &Job{
		ID:          f["id"],
		Type:        f["type"],
		State:       State(f["state"]),
		ClaimedBy:   f["claimed_by"],
		LastError:   f["last_error"],
		Attempts:    atoiOr(f["attempts"], 0),
		MaxAttempts: atoiOr(f["max_attempts"], 1),
	}
	if p := f["payload"]; p != "" {
		job.Payload = json.RawMessage(p)
	}
	var err error
	if job.CreatedAt, err = parseTime(f["created_at"]); err != nil {
		return nil, fmt.Errorf("job %s: created_at: %w", job.ID, err)
	}
	if job.ClaimedAt, err = parseTimePtr(f["claimed_at"]); err != nil {
		return nil, fmt.Errorf("job %s: claimed_at: %w", job.ID, err)
	}
	if job.NotBefore, err = parseTimePtr(f["not_before"]); err != nil {
		return nil, fmt.Errorf("job %s: not_before: %w", job.ID, err)
	}
	return job, nil
}

// flatToMap converts the flat [field, value, field, value] array that
// HGETALL returns from a Lua script into a map.
func flatToMap(v any) (map[string]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected script result type %T", v)
	}
	if len(arr)%2 != 0 {
		return nil, fmt.Errorf("odd-length hash result (%d entries)", len(arr))
	}
	out := make(map[string]string, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		k, _ := arr[i].(string)
		val, _ := arr[i+1].(string)
		out[k] = val
	}
	return out, nil
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func parseTimePtr(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// KeyTTL reports the remaining lifetime of a job's hash, or a negative
// duration if it has none. Exposed for tests and for operators checking
// that retention is actually being applied.
func (q *RedisQueue) KeyTTL(ctx context.Context, jobID string) (time.Duration, error) {
	return q.rdb.TTL(ctx, q.jobKey(jobID)).Result()
}
