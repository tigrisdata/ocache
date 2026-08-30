package ring

// Based on https://raw.githubusercontent.com/stathat/consistent/master/consistent.go

import (
	"container/heap"
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/dskit/flagext"
	dsmath "github.com/grafana/dskit/internal/math"
	"github.com/grafana/dskit/internal/slices"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	shardUtil "github.com/grafana/dskit/ring/shard"
	"github.com/grafana/dskit/services"
)

const (
	unhealthy = "Unhealthy"

	// GetBufferSize is the suggested size of buffers passed to Ring.Get(). It's based on
	// a typical replication factor 3, plus extra room for a JOINING + LEAVING instance.
	GetBufferSize = 5
)

// ReadRing represents the read interface to the ring.
type ReadRing interface {
	// Get returns n (or more) instances which form the replicas for the given key.
	// bufDescs, bufHosts and bufZones are slices to be overwritten for the return value
	// to avoid memory allocation; can be nil, or created with ring.MakeBuffersForGet().
	Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts, bufZones []string) (ReplicationSet, error)

	// GetAllHealthy returns all healthy instances in the ring, for the given operation.
	// This function doesn't check if the quorum is honored, so doesn't fail if the number
	// of unhealthy instances is greater than the tolerated max unavailable.
	GetAllHealthy(op Operation) (ReplicationSet, error)

	// GetReplicationSetForOperation returns all instances where the input operation should be executed.
	// The resulting ReplicationSet doesn't necessarily contains all healthy instances
	// in the ring, but could contain the minimum set of instances required to execute
	// the input operation.
	GetReplicationSetForOperation(op Operation) (ReplicationSet, error)

	ReplicationFactor() int

	// InstancesCount returns the number of instances in the ring.
	InstancesCount() int

	// ShuffleShard returns a subring for the provided identifier (eg. a tenant ID)
	// and size (number of instances).
	ShuffleShard(identifier string, size int) ReadRing

	// GetInstanceState returns the current state of an instance or an error if the
	// instance does not exist in the ring.
	GetInstanceState(instanceID string) (InstanceState, error)

	// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes
	// all instances that have been part of the identifier's shard since "now - lookbackPeriod".
	ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing

	// HasInstance returns whether the ring contains an instance matching the provided instanceID.
	HasInstance(instanceID string) bool

	// CleanupShuffleShardCache should delete cached shuffle-shard subrings for given identifier.
	CleanupShuffleShardCache(identifier string)

	// GetTokenRangesForInstance returns the token ranges owned by an instance in the ring
	GetTokenRangesForInstance(instanceID string) (TokenRanges, error)
}

var (
	// Write operation that also extends replica set, if instance state is not ACTIVE.
	Write = NewOp([]InstanceState{ACTIVE}, func(s InstanceState) bool {
		// We do not want to Write to instances that are not ACTIVE, but we do want
		// to write the extra replica somewhere.  So we increase the size of the set
		// of replicas for the key.
		// NB unhealthy instances will be filtered later by defaultReplicationStrategy.Filter().
		return s != ACTIVE
	})

	// WriteNoExtend is like Write, but with no replicaset extension.
	WriteNoExtend = NewOp([]InstanceState{ACTIVE}, nil)

	// Read operation that extends the replica set if an instance is not ACTIVE or LEAVING
	Read = NewOp([]InstanceState{ACTIVE, PENDING, LEAVING}, func(s InstanceState) bool {
		// To match Write with extended replica set we have to also increase the
		// size of the replica set for Read, but we can read from LEAVING ingesters.
		return s != ACTIVE && s != LEAVING
	})

	// Reporting is a special value for inquiring about health.
	Reporting = allStatesRingOperation
)

var (
	// ErrEmptyRing is the error returned when trying to get an element when nothing has been added to hash.
	ErrEmptyRing = errors.New("empty ring")

	// ErrInstanceNotFound is the error returned when trying to get information for an instance
	// not registered within the ring.
	ErrInstanceNotFound = errors.New("instance not found in the ring")

	// ErrTooManyUnhealthyInstances is the error returned when there are too many failed instances for a
	// specific operation.
	ErrTooManyUnhealthyInstances = errors.New("too many unhealthy instances in the ring")

	// ErrInconsistentTokensInfo is the error returned if, due to an internal bug, the mapping between
	// a token and its own instance is missing or unknown.
	ErrInconsistentTokensInfo = errors.New("inconsistent ring tokens information")
)

// Config for a Ring
type Config struct {
	KVStore              kv.Config              `yaml:"kvstore"`
	HeartbeatTimeout     time.Duration          `yaml:"heartbeat_timeout" category:"advanced"`
	ReplicationFactor    int                    `yaml:"replication_factor"`
	ZoneAwarenessEnabled bool                   `yaml:"zone_awareness_enabled"`
	ExcludedZones        flagext.StringSliceCSV `yaml:"excluded_zones" category:"advanced"`

	// Whether the shuffle-sharding subring cache is disabled. This option is set
	// internally and never exposed to the user.
	SubringCacheDisabled bool `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, "collectors/", f)

	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes. 0 = never (timeout disabled).")
	f.IntVar(&cfg.ReplicationFactor, prefix+"distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, prefix+"distributor.zone-awareness-enabled", false, "True to enable the zone-awareness and replicate ingested samples across different availability zones.")
	f.Var(&cfg.ExcludedZones, prefix+"distributor.excluded-zones", "Comma-separated list of zones to exclude from the ring. Instances in excluded zones will be filtered out from the ring.")
}

type instanceInfo struct {
	InstanceID string
	Zone       string
}

type instanceLiveness struct {
	state     atomic.Int32
	timestamp atomic.Int64
}

func newInstanceLiveness(instance InstanceDesc) *instanceLiveness {
	live := &instanceLiveness{}
	live.state.Store(int32(instance.State))
	live.timestamp.Store(instance.Timestamp)
	return live
}

func (l *instanceLiveness) snapshot(instance InstanceDesc) InstanceDesc {
	instance.State = InstanceState(l.state.Load())
	instance.Timestamp = l.timestamp.Load()
	return instance
}

type livenessMap map[string]*instanceLiveness

type livenessStore struct {
	slots atomic.Pointer[livenessMap]
}

func newLivenessStore(slots livenessMap) *livenessStore {
	store := &livenessStore{}
	store.slots.Store(&slots)
	return store
}

func (s *livenessStore) load(id string) *instanceLiveness {
	if s == nil {
		return nil
	}
	slots := s.slots.Load()
	if slots == nil {
		return nil
	}
	return (*slots)[id]
}

func (s *livenessStore) replace(slots livenessMap) {
	s.slots.Store(&slots)
}

type metricTimestamp struct {
	id         string
	timestamp  int64
	generation uint64
}

type metricTimestampHeap []metricTimestamp

func (h metricTimestampHeap) Len() int { return len(h) }
func (h metricTimestampHeap) Less(i, j int) bool {
	if h[i].timestamp == h[j].timestamp {
		return h[i].id < h[j].id
	}
	return h[i].timestamp < h[j].timestamp
}
func (h metricTimestampHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *metricTimestampHeap) Push(x interface{}) {
	*h = append(*h, x.(metricTimestamp))
}
func (h *metricTimestampHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Ring is a Service that maintains an in-memory copy of a ring and watches for changes.
type Ring struct {
	services.Service

	key      string
	cfg      Config
	KVClient kv.Client
	strategy ReplicationStrategy

	mtx              sync.RWMutex
	ringDesc         *Desc
	ringTokens       []uint32
	ringTokensByZone map[string][]uint32

	// liveness contains the mutable state and heartbeat timestamp for each
	// instance. The topology descriptor and all token indexes remain immutable
	// between full ring replacements.
	liveness      *livenessStore
	deltaSequence uint64

	// Oldest value of RegisteredTimestamp from all instances. If any instance had RegisteredTimestamp == 0,
	// then this value will be 0.
	oldestRegisteredTimestamp int64

	// Maps a token with the information of the instance holding it. This map is immutable and
	// cannot be changed in place because it's shared "as is" between subrings (the only way to
	// change it is to create a new one and replace it).
	ringInstanceByToken map[uint32]instanceInfo

	// When did a set of instances change the last time (instance changing state or heartbeat is ignored for this timestamp).
	lastTopologyChange time.Time

	// List of zones for which there's at least 1 instance in the ring. This list is guaranteed
	// to be sorted alphabetically.
	ringZones []string

	// Cache of shuffle-sharded subrings per identifier. Invalidated when topology changes.
	// If set to nil, no caching is done (used by tests, and subrings).
	shuffledSubringCache             map[subringCacheKey]*Ring
	shuffledSubringWithLookbackCache map[subringCacheKey]cachedSubringWithLookback

	numMembersGaugeVec      *prometheus.GaugeVec
	totalTokensGauge        prometheus.Gauge
	oldestTimestampGaugeVec *prometheus.GaugeVec

	metricsMu           sync.Mutex
	metricBuckets       map[string]string
	metricTimestamps    map[string]int64
	metricGenerations   map[string]uint64
	metricCounts        map[string]int
	metricTimestampHeap map[string]*metricTimestampHeap

	expiryMu   sync.Mutex
	nextExpiry time.Time
	expiryWake chan struct{}

	logger log.Logger
}

type subringCacheKey struct {
	identifier     string
	shardSize      int
	lookbackPeriod time.Duration
}

type cachedSubringWithLookback struct {
	subring                               *Ring
	validForLookbackWindowsStartingAfter  int64 // if the lookback window is from T to S, validForLookbackWindowsStartingAfter is the earliest value of T this cache entry is valid for
	validForLookbackWindowsStartingBefore int64 // if the lookback window is from T to S, validForLookbackWindowsStartingBefore is the latest value of T this cache entry is valid for
}

// New creates a new Ring. Being a service, Ring needs to be started to do anything.
func New(cfg Config, name, key string, logger log.Logger, reg prometheus.Registerer) (*Ring, error) {
	codec := GetCodec()
	// Suffix all client names with "-ring" to denote this kv client is used by the ring
	store, err := kv.NewClient(
		cfg.KVStore,
		codec,
		kv.RegistererWithKVName(reg, name+"-ring"),
		logger,
	)
	if err != nil {
		return nil, err
	}

	return NewWithStoreClientAndStrategy(cfg, name, key, store, NewDefaultReplicationStrategy(), reg, logger)
}

func NewWithStoreClientAndStrategy(cfg Config, name, key string, store kv.Client, strategy ReplicationStrategy, reg prometheus.Registerer, logger log.Logger) (*Ring, error) {
	if cfg.ReplicationFactor <= 0 {
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}

	r := &Ring{
		key:                              key,
		cfg:                              cfg,
		KVClient:                         store,
		strategy:                         strategy,
		ringDesc:                         &Desc{},
		liveness:                         newLivenessStore(make(livenessMap)),
		metricBuckets:                    make(map[string]string),
		metricTimestamps:                 make(map[string]int64),
		metricGenerations:                make(map[string]uint64),
		metricCounts:                     make(map[string]int),
		metricTimestampHeap:              make(map[string]*metricTimestampHeap),
		expiryWake:                       make(chan struct{}, 1),
		shuffledSubringCache:             map[subringCacheKey]*Ring{},
		shuffledSubringWithLookbackCache: map[subringCacheKey]cachedSubringWithLookback{},
		numMembersGaugeVec: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "ring_members",
			Help:        "Number of members in the ring",
			ConstLabels: map[string]string{"name": name},
		},
			[]string{"state"}),
		totalTokensGauge: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name:        "ring_tokens_total",
			Help:        "Number of tokens in the ring",
			ConstLabels: map[string]string{"name": name},
		}),
		oldestTimestampGaugeVec: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "ring_oldest_member_timestamp",
			Help:        "Timestamp of the oldest member in the ring.",
			ConstLabels: map[string]string{"name": name},
		},
			[]string{"state"}),
		logger: logger,
	}

	r.Service = services.NewBasicService(r.starting, r.loop, nil).WithName(fmt.Sprintf("%s ring client", name))
	return r, nil
}

func (r *Ring) starting(ctx context.Context) error {
	// Get the initial ring state so that, as soon as the service will be running, the in-memory
	// ring would be already populated and there's no race condition between when the service is
	// running and the WatchKey() callback is called for the first time.
	value, err := r.KVClient.Get(ctx, r.key)
	if err != nil {
		return errors.Wrap(err, "unable to initialise ring state")
	}
	if value != nil {
		desc, ok := value.(*Desc)
		if !ok {
			return fmt.Errorf("unable to initialise ring state: expected *ring.Desc, got %T", value)
		}
		r.updateRingState(desc)
	} else {
		level.Info(r.logger).Log("msg", "ring doesn't exist in KV store yet")
	}
	return nil
}

func (r *Ring) loop(ctx context.Context) error {
	// Update the ring metrics at start of the main loop.
	r.mtx.Lock()
	r.updateRingMetrics(Different)
	r.mtx.Unlock()

	go r.expiryLoop(ctx)

	if watcher, ok := r.KVClient.(interface {
		WatchKeyWithChanges(context.Context, string, func(memberlist.WatchKeyChange) bool)
	}); ok {
		watcher.WatchKeyWithChanges(ctx, r.key, func(change memberlist.WatchKeyChange) bool {
			return r.updateRingChange(ctx, change)
		})
		return nil
	}

	r.KVClient.WatchKey(ctx, r.key, func(value interface{}) bool {
		if value == nil {
			level.Info(r.logger).Log("msg", "ring doesn't exist in KV store yet")
			return true
		}

		desc, ok := value.(*Desc)
		if !ok {
			level.Warn(r.logger).Log("msg", "ignoring invalid ring value", "type", fmt.Sprintf("%T", value))
			return true
		}
		r.updateRingState(desc)
		return true
	})
	return nil
}

func (r *Ring) updateRingStateWithSequence(ringDesc *Desc, sequence uint64) {
	r.updateRingState(ringDesc)
	if sequence == 0 {
		return
	}

	r.mtx.Lock()
	if sequence > r.deltaSequence {
		r.deltaSequence = sequence
	}
	r.mtx.Unlock()
	r.recomputeNextExpiry()
}

func (r *Ring) updateRingChange(ctx context.Context, change memberlist.WatchKeyChange) bool {
	if change.FullSnapshot {
		if change.Value == nil {
			r.updateRingStateWithSequence(NewDesc(), change.Sequence)
			return true
		}

		desc, ok := change.Value.(*Desc)
		if !ok {
			level.Warn(r.logger).Log("msg", "ignoring invalid ring snapshot", "type", fmt.Sprintf("%T", change.Value))
			return true
		}
		r.updateRingStateWithSequence(desc, change.Sequence)
		return true
	}

	delta, ok := change.Value.(*Desc)
	if !ok || delta == nil || change.Sequence == 0 || len(change.ChangedKeys) == 0 {
		return r.recoverRingState(ctx, change.Sequence)
	}

	if r.applyRingDelta(delta, change.ChangedKeys, change.Sequence, change.TopologyChanged) {
		return true
	}

	return r.recoverRingState(ctx, change.Sequence)
}

func (r *Ring) recoverRingState(ctx context.Context, sequence uint64) bool {
	var (
		value   interface{}
		version uint64
		err     error
	)

	if versioned, ok := r.KVClient.(interface {
		GetWithVersion(context.Context, string) (interface{}, uint64, error)
	}); ok {
		value, version, err = versioned.GetWithVersion(ctx, r.key)
	} else {
		value, err = r.KVClient.Get(ctx, r.key)
		version = sequence
	}
	if err != nil {
		level.Warn(r.logger).Log("msg", "unable to recover ring snapshot after a memberlist delta gap", "err", err)
		return true
	}
	if value == nil {
		r.updateRingStateWithSequence(NewDesc(), version)
		return true
	}

	desc, ok := value.(*Desc)
	if !ok {
		level.Warn(r.logger).Log("msg", "ignoring invalid recovered ring snapshot", "type", fmt.Sprintf("%T", value))
		return true
	}
	r.updateRingStateWithSequence(desc, version)
	return true
}

func (r *Ring) applyRingDelta(delta *Desc, changedIDs []string, sequence uint64, topologyChanged bool) bool {
	if topologyChanged {
		return false
	}

	// Delta application updates the sequence and the shared liveness slots as
	// one unit. Use the write lock so a recovery or another watcher cannot
	// interleave a sequence check with the patch.
	r.mtx.Lock()
	if sequence <= r.deltaSequence {
		r.mtx.Unlock()
		return true
	}
	if r.deltaSequence == 0 {
		if sequence != 1 {
			r.mtx.Unlock()
			return false
		}
	} else if sequence != r.deltaSequence+1 {
		r.mtx.Unlock()
		return false
	}
	if r.ringDesc == nil || len(changedIDs) == 0 || len(changedIDs) != len(delta.Ingesters) {
		r.mtx.Unlock()
		return false
	}

	var seen map[string]struct{}
	if len(changedIDs) > 1 {
		seen = make(map[string]struct{}, len(changedIDs))
	}
	for _, id := range changedIDs {
		if seen != nil {
			if _, duplicate := seen[id]; duplicate {
				r.mtx.Unlock()
				return false
			}
			seen[id] = struct{}{}
		}

		incoming, ok := delta.Ingesters[id]
		static, present := r.ringDesc.Ingesters[id]
		if !ok || !present || r.liveness.load(id) == nil {
			r.mtx.Unlock()
			return false
		}
		if incoming.Id != "" && (incoming.Id != static.Id || incoming.Id != id) {
			r.mtx.Unlock()
			return false
		}
		// TopologyChanged is set by Desc.Merge before the change reaches this
		// path. These immutable-field checks also protect the reader from
		// applying a change produced by another implementation of the optional API.
		if incoming.Addr != static.Addr || incoming.Zone != static.Zone || incoming.RegisteredTimestamp != static.RegisteredTimestamp || !tokensEqual(incoming.Tokens, static.Tokens) {
			r.mtx.Unlock()
			return false
		}
	}
	if seen != nil {
		for id := range delta.Ingesters {
			if _, ok := seen[id]; !ok {
				r.mtx.Unlock()
				return false
			}
		}
	}

	now := time.Now()
	r.metricsMu.Lock()
	for id, incoming := range delta.Ingesters {
		static := r.ringDesc.Ingesters[id]
		slot := r.liveness.load(id)
		old := slot.snapshot(static)
		if incoming.Timestamp < old.Timestamp {
			r.metricsMu.Unlock()
			r.mtx.Unlock()
			return false
		}

		slot.state.Store(int32(incoming.State))
		slot.timestamp.Store(incoming.Timestamp)
		updated := static
		updated.State = incoming.State
		updated.Timestamp = incoming.Timestamp
		r.updateMetricInstanceLocked(id, updated, now)
	}
	r.emitMetricsLocked()
	r.metricsMu.Unlock()
	r.deltaSequence = sequence
	r.mtx.Unlock()

	for _, incoming := range delta.Ingesters {
		r.scheduleExpiry(incoming.Timestamp)
	}
	return true
}

func (r *Ring) instanceDescLocked(id string) InstanceDesc {
	instance := r.ringDesc.Ingesters[id]
	if slot := r.liveness.load(id); slot != nil {
		return slot.snapshot(instance)
	}
	return instance
}

func newLivenessMap(desc *Desc) livenessMap {
	slots := make(livenessMap, len(desc.Ingesters))
	for id, instance := range desc.Ingesters {
		slots[id] = newInstanceLiveness(instance)
	}
	return slots
}

func (r *Ring) updateRingState(ringDesc *Desc) {
	if ringDesc == nil {
		ringDesc = NewDesc()
	}
	if ringDesc.Ingesters == nil {
		ringDesc.Ingesters = make(map[string]InstanceDesc)
	}

	r.mtx.RLock()
	prevRing := r.ringDesc
	if prevRing == nil {
		prevRing = NewDesc()
	}
	r.mtx.RUnlock()

	// Filter out all instances belonging to excluded zones.
	if len(r.cfg.ExcludedZones) > 0 {
		for instanceID, instance := range ringDesc.Ingesters {
			if slices.Contains(r.cfg.ExcludedZones, instance.Zone) {
				delete(ringDesc.Ingesters, instanceID)
			}
		}
	}

	// Ensure the ID of each InstanceDesc is set based on the map of instances. This
	// handles the case where some components are running older lifecyclers which do
	// not set the instance ID when registering in the ring.
	ringDesc.setInstanceIDs()

	rc := prevRing.RingCompare(ringDesc)
	if rc == Equal || rc == EqualButStatesAndTimestamps {
		// No need to update tokens or zones. Only states and timestamps
		// have changed. (If Equal, nothing has changed, but that doesn't happen
		// when watching the ring for updates).
		r.mtx.Lock()
		r.ringDesc = ringDesc
		r.updateLivenessSnapshotLocked(ringDesc)
		r.updateRingMetrics(rc)
		r.mtx.Unlock()
		r.recomputeNextExpiry()
		return
	}

	now := time.Now()
	ringTokens := ringDesc.GetTokens()
	ringTokensByZone := ringDesc.getTokensByZone()
	ringInstanceByToken := ringDesc.getTokensInfo()
	ringZones := getZones(ringTokensByZone)
	oldestRegisteredTimestamp := ringDesc.getOldestRegisteredTimestamp()
	slots := newLivenessMap(ringDesc)

	r.mtx.Lock()
	r.ringDesc = ringDesc
	r.ringTokens = ringTokens
	r.ringTokensByZone = ringTokensByZone
	r.ringInstanceByToken = ringInstanceByToken
	r.ringZones = ringZones
	r.oldestRegisteredTimestamp = oldestRegisteredTimestamp
	r.lastTopologyChange = now
	if r.liveness == nil {
		r.liveness = newLivenessStore(slots)
	} else {
		r.liveness.replace(slots)
	}

	// Invalidate all cached subrings.
	if r.shuffledSubringCache != nil {
		r.shuffledSubringCache = make(map[subringCacheKey]*Ring)
	}
	if r.shuffledSubringWithLookbackCache != nil {
		r.shuffledSubringWithLookbackCache = make(map[subringCacheKey]cachedSubringWithLookback)
	}

	r.updateRingMetrics(rc)
	r.mtx.Unlock()
	r.recomputeNextExpiry()
}

func (r *Ring) updateLivenessSnapshotLocked(desc *Desc) {
	if r.liveness == nil {
		r.liveness = newLivenessStore(newLivenessMap(desc))
		return
	}

	old := r.liveness.slots.Load()
	slots := make(livenessMap, len(desc.Ingesters))
	for id, instance := range desc.Ingesters {
		if old != nil && (*old)[id] != nil {
			slots[id] = (*old)[id]
			slots[id].state.Store(int32(instance.State))
			slots[id].timestamp.Store(instance.Timestamp)
		} else {
			slots[id] = newInstanceLiveness(instance)
		}
	}
	r.liveness.replace(slots)
}

// Get returns n (or more) instances which form the replicas for the given key.
func (r *Ring) Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts, bufZones []string) (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if r.ringDesc == nil || len(r.ringTokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	instances, err := r.findInstancesForKey(key, op, bufDescs, bufHosts, bufZones, nil)
	if err != nil {
		return ReplicationSet{}, err
	}

	healthyInstances, maxFailure, err := r.strategy.Filter(instances, op, r.cfg.ReplicationFactor, r.cfg.HeartbeatTimeout, r.cfg.ZoneAwarenessEnabled)
	if err != nil {
		return ReplicationSet{}, err
	}

	return ReplicationSet{
		Instances: healthyInstances,
		MaxErrors: maxFailure,
	}, nil
}

// Returns instances for given key and operation. Instances are not filtered through ReplicationStrategy.
// InstanceFilter can ignore uninteresting instances that would otherwise be part of the output, and can also stop search early.
// This function needs to be called with read lock on the ring.
func (r *Ring) findInstancesForKey(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts []string, bufZones []string, instanceFilter func(instanceID string) (include, keepGoing bool)) ([]InstanceDesc, error) {
	var (
		n            = r.cfg.ReplicationFactor
		instances    = bufDescs[:0]
		start        = searchToken(r.ringTokens, key)
		iterations   = 0
		maxZones     = len(r.ringTokensByZone)
		maxInstances = len(r.ringDesc.Ingesters)

		// We use a slice instead of a map because it's faster to search within a
		// slice than lookup a map for a very low number of items.
		distinctHosts = bufHosts[:0]
		distinctZones = bufZones[:0]
	)
	for i := start; len(distinctHosts) < dsmath.Min(maxInstances, n) && len(distinctZones) < maxZones && iterations < len(r.ringTokens); i++ {
		iterations++
		// Wrap i around in the ring.
		i %= len(r.ringTokens)
		token := r.ringTokens[i]

		info, ok := r.ringInstanceByToken[token]
		if !ok {
			// This should never happen unless a bug in the ring code.
			return nil, ErrInconsistentTokensInfo
		}

		// We want n *distinct* instances && distinct zones.
		if slices.Contains(distinctHosts, info.InstanceID) {
			continue
		}

		// Ignore if the instances don't have a zone set.
		if r.cfg.ZoneAwarenessEnabled && info.Zone != "" {
			if slices.Contains(distinctZones, info.Zone) {
				continue
			}
		}

		distinctHosts = append(distinctHosts, info.InstanceID)
		instance := r.instanceDescLocked(info.InstanceID)

		// Check whether the replica set should be extended given we're including
		// this instance.
		if op.ShouldExtendReplicaSetOnState(instance.State) {
			n++
		} else if r.cfg.ZoneAwarenessEnabled && info.Zone != "" {
			// We should only add the zone if we are not going to extend,
			// as we want to extend the instance in the same AZ.
			distinctZones = append(distinctZones, info.Zone)
		}

		include, keepGoing := true, true
		if instanceFilter != nil {
			include, keepGoing = instanceFilter(info.InstanceID)
		}
		if include {
			instances = append(instances, instance)
		}
		if !keepGoing {
			break
		}
	}
	return instances, nil
}

// GetAllHealthy implements ReadRing.
func (r *Ring) GetAllHealthy(op Operation) (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.ringDesc == nil || len(r.ringDesc.Ingesters) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	now := time.Now()
	instances := make([]InstanceDesc, 0, len(r.ringDesc.Ingesters))
	for id := range r.ringDesc.Ingesters {
		instance := r.instanceDescLocked(id)
		if r.IsHealthy(&instance, op, now) {
			instances = append(instances, instance)
		}
	}

	return ReplicationSet{
		Instances: instances,
		MaxErrors: 0,
	}, nil
}

// GetReplicationSetForOperation implements ReadRing.
func (r *Ring) GetReplicationSetForOperation(op Operation) (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.ringDesc == nil || len(r.ringTokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	// Build the initial replication set, excluding unhealthy instances.
	healthyInstances := make([]InstanceDesc, 0, len(r.ringDesc.Ingesters))
	zoneFailures := make(map[string]struct{})
	now := time.Now()

	for id := range r.ringDesc.Ingesters {
		instance := r.instanceDescLocked(id)
		if r.IsHealthy(&instance, op, now) {
			healthyInstances = append(healthyInstances, instance)
		} else {
			zoneFailures[instance.Zone] = struct{}{}
		}
	}

	// Max errors and max unavailable zones are mutually exclusive. We initialise both
	// to 0 and then we update them whether zone-awareness is enabled or not.
	maxErrors := 0
	maxUnavailableZones := 0

	if r.cfg.ZoneAwarenessEnabled {
		// Given data is replicated to RF different zones, we can tolerate a number of
		// RF/2 failing zones. However, we need to protect from the case the ring currently
		// contains instances in a number of zones < RF.
		numReplicatedZones := dsmath.Min(len(r.ringZones), r.cfg.ReplicationFactor)
		minSuccessZones := (numReplicatedZones / 2) + 1
		maxUnavailableZones = minSuccessZones - 1

		if len(zoneFailures) > maxUnavailableZones {
			return ReplicationSet{}, ErrTooManyUnhealthyInstances
		}

		if len(zoneFailures) > 0 {
			// We remove all instances (even healthy ones) from zones with at least
			// 1 failing instance. Due to how replication works when zone-awareness is
			// enabled (data is replicated to RF different zones), there's no benefit in
			// querying healthy instances from "failing zones". A zone is considered
			// failed if there is single error.
			filteredInstances := make([]InstanceDesc, 0, len(r.ringDesc.Ingesters))
			for _, instance := range healthyInstances {
				if _, ok := zoneFailures[instance.Zone]; !ok {
					filteredInstances = append(filteredInstances, instance)
				}
			}

			healthyInstances = filteredInstances
		}

		// Since we removed all instances from zones containing at least 1 failing
		// instance, we have to decrease the max unavailable zones accordingly.
		maxUnavailableZones -= len(zoneFailures)
	} else {
		// Calculate the number of required instances;
		// ensure we always require at least RF-1 when RF=3.
		numRequired := len(r.ringDesc.Ingesters)
		if numRequired < r.cfg.ReplicationFactor {
			numRequired = r.cfg.ReplicationFactor
		}
		// We can tolerate this many failures
		numRequired -= r.cfg.ReplicationFactor / 2

		if len(healthyInstances) < numRequired {
			return ReplicationSet{}, ErrTooManyUnhealthyInstances
		}

		maxErrors = len(healthyInstances) - numRequired
	}

	return ReplicationSet{
		Instances:            healthyInstances,
		MaxErrors:            maxErrors,
		MaxUnavailableZones:  maxUnavailableZones,
		ZoneAwarenessEnabled: r.cfg.ZoneAwarenessEnabled,
	}, nil
}

// CountTokens returns the number tokens within the range for each instance.
// In case of zone-awareness, this method takes into account only tokens of
// the same zone. More precisely, for each instance only the distance between
// its tokens and tokens of the instances from the same zone will be considered.
func (r *Desc) CountTokens() map[string]int64 {
	var (
		owned               = make(map[string]int64, len(r.Ingesters))
		ringTokensByZone    = r.getTokensByZone()
		ringInstanceByToken = r.getTokensInfo()
	)

	for _, ringTokens := range ringTokensByZone {
		for i, token := range ringTokens {
			var prevToken uint32

			// Compute how many tokens are within the range.
			if i == 0 {
				prevToken = ringTokens[len(ringTokens)-1]
			} else {
				prevToken = ringTokens[i-1]
			}

			diff := tokenDistance(prevToken, token)
			info := ringInstanceByToken[token]
			owned[info.InstanceID] = owned[info.InstanceID] + diff
		}
	}

	// Set to 0 the number of owned tokens by instances which don't have tokens yet.
	for id := range r.Ingesters {
		if _, ok := owned[id]; !ok {
			owned[id] = 0
		}
	}

	return owned
}

var metricStates = []string{unhealthy, ACTIVE.String(), LEAVING.String(), PENDING.String(), JOINING.String()}

func (r *Ring) metricState(instance InstanceDesc, now time.Time) string {
	if !instance.IsHealthy(Reporting, r.cfg.HeartbeatTimeout, now) {
		return unhealthy
	}
	return instance.State.String()
}

func (r *Ring) updateMetricInstanceLocked(id string, instance InstanceDesc, now time.Time) {
	if r.metricBuckets == nil {
		r.metricBuckets = make(map[string]string)
		r.metricTimestamps = make(map[string]int64)
		r.metricGenerations = make(map[string]uint64)
		r.metricCounts = make(map[string]int)
		r.metricTimestampHeap = make(map[string]*metricTimestampHeap)
	}

	bucket := r.metricState(instance, now)
	oldBucket, hadBucket := r.metricBuckets[id]
	oldTimestamp := r.metricTimestamps[id]
	if hadBucket && oldBucket == bucket && oldTimestamp == instance.Timestamp {
		return
	}
	if hadBucket {
		r.metricCounts[oldBucket]--
	}

	r.metricBuckets[id] = bucket
	r.metricTimestamps[id] = instance.Timestamp
	r.metricGenerations[id]++
	r.metricCounts[bucket]++
	if r.metricTimestampHeap[bucket] == nil {
		r.metricTimestampHeap[bucket] = &metricTimestampHeap{}
	}
	heap.Push(r.metricTimestampHeap[bucket], metricTimestamp{
		id:         id,
		timestamp:  instance.Timestamp,
		generation: r.metricGenerations[id],
	})
}

func (r *Ring) metricOldestTimestampLocked(state string) int64 {
	h := r.metricTimestampHeap[state]
	for h != nil && h.Len() > 0 {
		entry := (*h)[0]
		if r.metricBuckets[entry.id] != state || r.metricTimestamps[entry.id] != entry.timestamp || r.metricGenerations[entry.id] != entry.generation {
			heap.Pop(h)
			continue
		}
		return entry.timestamp
	}
	return 0
}

func (r *Ring) emitMetricsLocked() {
	for _, state := range metricStates {
		r.numMembersGaugeVec.WithLabelValues(state).Set(float64(r.metricCounts[state]))
		r.oldestTimestampGaugeVec.WithLabelValues(state).Set(float64(r.metricOldestTimestampLocked(state)))
	}
}

func (r *Ring) rebuildMetricsLocked(now time.Time) {
	r.metricsMu.Lock()
	r.metricBuckets = make(map[string]string, len(r.ringDesc.Ingesters))
	r.metricTimestamps = make(map[string]int64, len(r.ringDesc.Ingesters))
	r.metricGenerations = make(map[string]uint64, len(r.ringDesc.Ingesters))
	r.metricCounts = make(map[string]int, len(metricStates))
	r.metricTimestampHeap = make(map[string]*metricTimestampHeap, len(metricStates))
	for id := range r.ringDesc.Ingesters {
		r.updateMetricInstanceLocked(id, r.instanceDescLocked(id), now)
	}
	r.emitMetricsLocked()
	r.metricsMu.Unlock()
}

// updateRingMetrics updates ring metrics. Caller must be holding the Write lock!
func (r *Ring) updateRingMetrics(compareResult CompareResult) {
	if compareResult == Equal {
		return
	}

	r.rebuildMetricsLocked(time.Now())
	if compareResult != EqualButStatesAndTimestamps {
		r.totalTokensGauge.Set(float64(len(r.ringTokens)))
	}
}

func (r *Ring) scheduleExpiry(timestamp int64) {
	if r.cfg.HeartbeatTimeout == 0 {
		return
	}
	deadline := time.Unix(timestamp, 0).Add(r.cfg.HeartbeatTimeout)
	if !deadline.After(time.Now()) {
		return
	}

	r.expiryMu.Lock()
	changed := r.nextExpiry.IsZero() || deadline.Before(r.nextExpiry)
	if changed {
		r.nextExpiry = deadline
	}
	r.expiryMu.Unlock()
	if changed {
		select {
		case r.expiryWake <- struct{}{}:
		default:
		}
	}
}

func (r *Ring) recomputeNextExpiry() {
	if r.cfg.HeartbeatTimeout == 0 {
		return
	}

	now := time.Now()
	var next time.Time
	r.mtx.RLock()
	if r.ringDesc != nil {
		for id := range r.ringDesc.Ingesters {
			instance := r.instanceDescLocked(id)
			deadline := time.Unix(instance.Timestamp, 0).Add(r.cfg.HeartbeatTimeout)
			if deadline.After(now) && (next.IsZero() || deadline.Before(next)) {
				next = deadline
			}
		}
	}
	r.mtx.RUnlock()

	r.expiryMu.Lock()
	r.nextExpiry = next
	r.expiryMu.Unlock()
	select {
	case r.expiryWake <- struct{}{}:
	default:
	}
}

func (r *Ring) expireMetrics(now time.Time) {
	r.mtx.RLock()
	if r.ringDesc == nil {
		r.mtx.RUnlock()
		return
	}
	r.metricsMu.Lock()
	for id := range r.ringDesc.Ingesters {
		r.updateMetricInstanceLocked(id, r.instanceDescLocked(id), now)
	}
	r.emitMetricsLocked()
	r.metricsMu.Unlock()
	r.mtx.RUnlock()
}

func (r *Ring) expiryLoop(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		r.expiryMu.Lock()
		next := r.nextExpiry
		r.expiryMu.Unlock()
		if next.IsZero() {
			select {
			case <-r.expiryWake:
				continue
			case <-ctx.Done():
				return
			}
		}

		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		timer.Reset(wait)
		select {
		case <-timer.C:
			r.expireMetrics(time.Now())
			r.recomputeNextExpiry()
		case <-r.expiryWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

// ShuffleShard returns a subring for the provided identifier (eg. a tenant ID)
// and size (number of instances). The size is expected to be a multiple of the
// number of zones and the returned subring will contain the same number of
// instances per zone as far as there are enough registered instances in the ring.
//
// The algorithm used to build the subring is a shuffle sharder based on probabilistic
// hashing. We treat each zone as a separate ring and pick N unique replicas from each
// zone, walking the ring starting from random but predictable numbers. The random
// generator is initialised with a seed based on the provided identifier.
//
// This implementation guarantees:
//
// - Stability: given the same ring, two invocations returns the same result.
//
// - Consistency: adding/removing 1 instance from the ring generates a resulting
// subring with no more then 1 difference.
//
// - Shuffling: probabilistically, for a large enough cluster each identifier gets a different
// set of instances, with a reduced number of overlapping instances between two identifiers.
func (r *Ring) ShuffleShard(identifier string, size int) ReadRing {
	// Nothing to do if the shard size is not smaller then the actual ring.
	if size <= 0 || r.InstancesCount() <= size {
		return r
	}

	if cached := r.getCachedShuffledSubring(identifier, size); cached != nil {
		return cached
	}

	result := r.shuffleShard(identifier, size, 0, time.Now())
	// Only cache subring if it is different from this ring, to avoid deadlocks in getCachedShuffledSubring,
	// when we update the cached ring.
	if result != r {
		r.setCachedShuffledSubring(identifier, size, result)
	}
	return result
}

// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes all instances
// that have been part of the identifier's shard since "now - lookbackPeriod".
//
// The returned subring may be unbalanced with regard to zones and should never be used for write
// operations (read only).
//
// This function supports caching, but the cache will only be effective if successive calls for the
// same identifier are for increasing values of (now-lookbackPeriod).
func (r *Ring) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing {
	// Nothing to do if the shard size is not smaller then the actual ring.
	if size <= 0 || r.InstancesCount() <= size {
		return r
	}

	if cached := r.getCachedShuffledSubringWithLookback(identifier, size, lookbackPeriod, now); cached != nil {
		return cached
	}

	result := r.shuffleShard(identifier, size, lookbackPeriod, now)

	if result != r {
		r.setCachedShuffledSubringWithLookback(identifier, size, lookbackPeriod, now, result)
	}

	return result
}

func (r *Ring) shuffleShard(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *Ring {
	lookbackUntil := now.Add(-lookbackPeriod).Unix()

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	// If all instances have RegisteredTimestamp within the lookback period,
	// then all instances would be included in the resulting ring, so we can
	// simply return this ring.
	//
	// If any instance had RegisteredTimestamp equal to 0 (it would not cause additional lookup of next instance),
	// then r.oldestRegisteredTimestamp is zero too, and we skip this optimization.
	if lookbackPeriod > 0 && r.oldestRegisteredTimestamp > 0 && r.oldestRegisteredTimestamp >= lookbackUntil {
		return r
	}

	var numInstancesPerZone int
	var actualZones []string

	if r.cfg.ZoneAwarenessEnabled {
		numInstancesPerZone = shardUtil.ShuffleShardExpectedInstancesPerZone(size, len(r.ringZones))
		actualZones = r.ringZones
	} else {
		numInstancesPerZone = size
		actualZones = []string{""}
	}

	shard := make(map[string]InstanceDesc, size)

	// We need to iterate zones always in the same order to guarantee stability.
	for _, zone := range actualZones {
		var tokens []uint32

		if r.cfg.ZoneAwarenessEnabled {
			tokens = r.ringTokensByZone[zone]
		} else {
			// When zone-awareness is disabled, we just iterate over 1 single fake zone
			// and use all tokens in the ring.
			tokens = r.ringTokens
		}

		// Initialise the random generator used to select instances in the ring.
		// Since we consider each zone like an independent ring, we have to use dedicated
		// pseudo-random generator for each zone, in order to guarantee the "consistency"
		// property when the shard size changes or a new zone is added.
		random := rand.New(rand.NewSource(shardUtil.ShuffleShardSeed(identifier, zone)))

		// To select one more instance while guaranteeing the "consistency" property,
		// we do pick a random value from the generator and resolve uniqueness collisions
		// (if any) continuing walking the ring.
		for i := 0; i < numInstancesPerZone; i++ {
			start := searchToken(tokens, random.Uint32())
			iterations := 0
			found := false

			for p := start; iterations < len(tokens); p++ {
				iterations++

				// Wrap p around in the ring.
				p %= len(tokens)

				info, ok := r.ringInstanceByToken[tokens[p]]
				if !ok {
					// This should never happen unless a bug in the ring code.
					panic(ErrInconsistentTokensInfo)
				}

				// Ensure we select a unique instance.
				if _, ok := shard[info.InstanceID]; ok {
					continue
				}

				instanceID := info.InstanceID
				instance := r.instanceDescLocked(instanceID)
				shard[instanceID] = instance

				// If the lookback is enabled and this instance has been registered within the lookback period
				// then we should include it in the subring but continuing selecting instances.
				if lookbackPeriod > 0 && instance.RegisteredTimestamp >= lookbackUntil {
					continue
				}

				found = true
				break
			}

			// If one more instance has not been found, we can stop looking for
			// more instances in this zone, because it means the zone has no more
			// instances which haven't been already selected.
			if !found {
				break
			}
		}
	}

	// Build a read-only ring for the shard.
	shardDesc := &Desc{Ingesters: shard}
	shardTokensByZone := shardDesc.getTokensByZone()
	shardTokens := mergeTokenGroups(shardTokensByZone)

	return &Ring{
		cfg:              r.cfg,
		strategy:         r.strategy,
		ringDesc:         shardDesc,
		liveness:         r.liveness,
		ringTokens:       shardTokens,
		ringTokensByZone: shardTokensByZone,
		ringZones:        getZones(shardTokensByZone),

		oldestRegisteredTimestamp: shardDesc.getOldestRegisteredTimestamp(),

		// We reference the original map as is in order to avoid copying. It's safe to do
		// because this map is immutable by design and it's a superset of the actual instances
		// with the subring.
		ringInstanceByToken: r.ringInstanceByToken,

		// For caching to work, remember these values.
		lastTopologyChange: r.lastTopologyChange,
	}
}

// mergeTokenGroups returns a sorted list of all tokens in each entry in groupsByName.
// Each element of groupsByName is assumed to already be sorted.
func mergeTokenGroups(groupsByName map[string][]uint32) []uint32 {
	tokenCount := 0
	groupsByIndex := make([][]uint32, 0, len(groupsByName))
	nextIndex := make([]int, 0, len(groupsByName))

	for _, group := range groupsByName {
		// If there's only one group, there's nothing to merge.
		if len(groupsByName) == 1 {
			return group
		}

		tokenCount += len(group)
		groupsByIndex = append(groupsByIndex, group)
		nextIndex = append(nextIndex, 0)
	}

	merged := make([]uint32, 0, tokenCount)

	for i := 0; i < tokenCount; i++ {
		haveSeenGroupWithRemainingToken := false
		lowestToken := uint32(0)
		lowestTokenGroupIndex := 0

		for groupIndex, group := range groupsByIndex {
			nextIndexInGroup := nextIndex[groupIndex]

			if nextIndexInGroup >= len(group) {
				continue
			}

			if group[nextIndexInGroup] < lowestToken || !haveSeenGroupWithRemainingToken {
				lowestToken = group[nextIndexInGroup]
				lowestTokenGroupIndex = groupIndex
				haveSeenGroupWithRemainingToken = true
			}
		}

		if !haveSeenGroupWithRemainingToken {
			return merged
		}

		merged = append(merged, lowestToken)
		nextIndex[lowestTokenGroupIndex]++
	}

	return merged
}

// GetInstanceState returns the current state of an instance or an error if the
// instance does not exist in the ring.
func (r *Ring) GetInstanceState(instanceID string) (InstanceState, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	instances := r.ringDesc.GetIngesters()
	instance, ok := instances[instanceID]
	if !ok {
		return PENDING, ErrInstanceNotFound
	}
	instance = r.instanceDescLocked(instanceID)
	return instance.GetState(), nil
}

// HasInstance returns whether the ring contains an instance matching the provided instanceID.
func (r *Ring) HasInstance(instanceID string) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	instances := r.ringDesc.GetIngesters()
	_, ok := instances[instanceID]
	return ok
}

func (r *Ring) getCachedShuffledSubring(identifier string, size int) *Ring {
	if r.cfg.SubringCacheDisabled {
		return nil
	}

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	// if shuffledSubringCache map is nil, reading it returns default value (nil pointer).
	cached := r.shuffledSubringCache[subringCacheKey{identifier: identifier, shardSize: size}]
	if cached == nil {
		return nil
	}

	// No need to update cached subring, if it is the original ring itself.
	if r == cached {
		return cached
	}

	// The cached subring shares the parent's atomic liveness slots, so a
	// heartbeat does not require copying state through the cached descriptor.
	return cached
}

func (r *Ring) setCachedShuffledSubring(identifier string, size int, subring *Ring) {
	if subring == nil || r.cfg.SubringCacheDisabled {
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	// Only cache if *this* ring hasn't changed since computing result
	// (which can happen between releasing the read lock and getting read-write lock).
	// Note that shuffledSubringCache can be only nil when set by test.
	if r.shuffledSubringCache != nil && r.lastTopologyChange.Equal(subring.lastTopologyChange) {
		r.shuffledSubringCache[subringCacheKey{identifier: identifier, shardSize: size}] = subring
	}
}

func (r *Ring) getCachedShuffledSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) *Ring {
	if r.cfg.SubringCacheDisabled {
		return nil
	}

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	cached, ok := r.shuffledSubringWithLookbackCache[subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod}]
	if !ok {
		return nil
	}

	lookbackWindowStart := now.Add(-lookbackPeriod).Unix()
	if lookbackWindowStart < cached.validForLookbackWindowsStartingAfter || lookbackWindowStart > cached.validForLookbackWindowsStartingBefore {
		// The cached subring is not valid for the lookback window that has been requested.
		return nil
	}

	cachedSubring := cached.subring

	// No need to update the cached subring if it is the original ring itself.
	if r == cachedSubring {
		return cachedSubring
	}

	// The cached subring shares the parent's atomic liveness slots, so a
	// heartbeat does not require copying state through the cached descriptor.
	return cachedSubring
}

func (r *Ring) setCachedShuffledSubringWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time, subring *Ring) {
	if subring == nil || r.cfg.SubringCacheDisabled {
		return
	}

	lookbackWindowStart := now.Add(-lookbackPeriod).Unix()
	validForLookbackWindowsStartingBefore := int64(math.MaxInt64)

	for _, instance := range subring.ringDesc.Ingesters {
		registeredDuringLookbackWindow := instance.RegisteredTimestamp >= lookbackWindowStart

		if registeredDuringLookbackWindow && instance.RegisteredTimestamp < validForLookbackWindowsStartingBefore {
			validForLookbackWindowsStartingBefore = instance.RegisteredTimestamp
		}
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	// Only cache if *this* ring hasn't changed since computing result
	// (which can happen between releasing the read lock and getting read-write lock).
	// Note that shuffledSubringWithLookbackCache can be only nil when set by test.
	if r.shuffledSubringWithLookbackCache == nil {
		return
	}

	if !r.lastTopologyChange.Equal(subring.lastTopologyChange) {
		return
	}

	// Only update cache if subring's lookback window starts later than the previously cached subring for this identifier,
	// if there is one. This prevents cache thrashing due to different calls competing if their lookback windows start
	// before and after the time of an instance registering.
	key := subringCacheKey{identifier: identifier, shardSize: size, lookbackPeriod: lookbackPeriod}

	if existingEntry, haveCached := r.shuffledSubringWithLookbackCache[key]; !haveCached || existingEntry.validForLookbackWindowsStartingAfter < lookbackWindowStart {
		r.shuffledSubringWithLookbackCache[key] = cachedSubringWithLookback{
			subring:                               subring,
			validForLookbackWindowsStartingAfter:  lookbackWindowStart,
			validForLookbackWindowsStartingBefore: validForLookbackWindowsStartingBefore,
		}
	}
}

func (r *Ring) CleanupShuffleShardCache(identifier string) {
	if r.cfg.SubringCacheDisabled {
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	for k := range r.shuffledSubringCache {
		if k.identifier == identifier {
			delete(r.shuffledSubringCache, k)
		}
	}

	for k := range r.shuffledSubringWithLookbackCache {
		if k.identifier == identifier {
			delete(r.shuffledSubringWithLookbackCache, k)
		}
	}
}

func (r *Ring) casRing(ctx context.Context, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	return r.KVClient.CAS(ctx, r.key, f)
}

func (r *Ring) getRing(_ context.Context) (*Desc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	ringDesc := proto.Clone(r.ringDesc).(*Desc)
	for id := range ringDesc.Ingesters {
		ringDesc.Ingesters[id] = r.instanceDescLocked(id)
	}

	return ringDesc, nil
}

func (r *Ring) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	newRingPageHandler(r, r.cfg.HeartbeatTimeout).handle(w, req)
}

// Operation describes which instances can be included in the replica set, based on their state.
//
// Implemented as bitmap, with upper 16-bits used for encoding extendReplicaSet, and lower 16-bits used for encoding healthy states.
type Operation uint32

// NewOp constructs new Operation with given "healthy" states for operation, and optional function to extend replica set.
// Result of calling shouldExtendReplicaSet is cached.
func NewOp(healthyStates []InstanceState, shouldExtendReplicaSet func(s InstanceState) bool) Operation {
	op := Operation(0)
	for _, s := range healthyStates {
		op |= (1 << s)
	}

	if shouldExtendReplicaSet != nil {
		for _, s := range []InstanceState{ACTIVE, LEAVING, PENDING, JOINING, LEFT} {
			if shouldExtendReplicaSet(s) {
				op |= (0x10000 << s)
			}
		}
	}

	return op
}

// IsInstanceInStateHealthy is used during "filtering" phase to remove undesired instances based on their state.
func (op Operation) IsInstanceInStateHealthy(s InstanceState) bool {
	return op&(1<<s) > 0
}

// ShouldExtendReplicaSetOnState returns true if given a state of instance that's going to be
// added to the replica set, the replica set size should be extended by 1
// more instance for the given operation.
func (op Operation) ShouldExtendReplicaSetOnState(s InstanceState) bool {
	return op&(0x10000<<s) > 0
}

// All states are healthy, no states extend replica set.
var allStatesRingOperation = Operation(0x0000ffff)

// numberOfKeysOwnedByInstance returns how many of the supplied keys are owned by given instance.
func (r *Ring) numberOfKeysOwnedByInstance(keys []uint32, op Operation, instanceID string, bufDescs []InstanceDesc, bufHosts []string, bufZones []string) (int, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.ringDesc == nil || len(r.ringTokens) == 0 {
		return 0, ErrEmptyRing
	}

	// Instance is not in this ring, it can't own any key.
	if _, ok := r.ringDesc.Ingesters[instanceID]; !ok {
		return 0, nil
	}

	owned := 0
	for _, tok := range keys {
		i, err := r.findInstancesForKey(tok, op, bufDescs, bufHosts, bufZones, func(foundInstanceID string) (include, keepGoing bool) {
			if foundInstanceID == instanceID {
				// If we've found our instance, we can stop.
				return true, false
			}
			return false, true
		})
		if err != nil {
			return 0, err
		}
		if len(i) > 0 {
			owned++
		}
	}
	return owned, nil
}
