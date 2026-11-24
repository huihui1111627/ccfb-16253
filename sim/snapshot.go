package sim

// EvictionRecord 一次摘除事件
type EvictionRecord struct {
	InstanceID   string
	Tick         int64
	DC           string
	SW           string
	Alive        bool // 摘除时实例是否实际还活着(误摘判定)
	InProtection bool
}

// KillRecord 一次真实宕机事件
type KillRecord struct {
	ID      string
	Tick    int64
	Service string
}

// InstanceSnap 单个实例的快照
type InstanceSnap struct {
	ID         string `json:"id"`
	Service    string `json:"svc"`
	DC         string `json:"dc"`
	SW         string `json:"sw"`
	Alive      bool   `json:"alive"`
	Registered bool   `json:"reg"`
	LastHBAge  int64  `json:"hbAge"` // 距最近一次心跳 ack 的 tick 数
}

// RegistrySnap 注册中心节点快照
type RegistrySnap struct {
	ID        string  `json:"id"`
	Protected bool    `json:"protected"`
	Degraded  bool    `json:"degraded"`
	Reason    string  `json:"reason"`
	Ratio     float64 `json:"ratio"`
	Actual    int64   `json:"actual"`
	Expected  int64   `json:"expected"`
	DirectHB  int64   `json:"directHB"`
	Known     int     `json:"known"`
	Evicted   int     `json:"evicted"`
	Inbox     int     `json:"inbox"`
}

// CallerSnap 调用方快照
type CallerSnap struct {
	ID            string         `json:"id"`
	DC            string         `json:"dc"`
	Service       string         `json:"svc"`
	Cached        int            `json:"cached"`
	Reachable     bool           `json:"reachable"`
	EmptyRejected int64          `json:"emptyRejected"`
	Unreachable   int64          `json:"unreachable"`
	Breakers      map[string]int `json:"breakers"` // 实例ID -> 熔断状态枚举
}

// Snapshot 一个时间点的全系统快照,供页面播放
type Snapshot struct {
	Tick       int64          `json:"tick"`
	Registries []RegistrySnap `json:"registries"`
	Instances  []InstanceSnap `json:"instances"`
	Callers    []CallerSnap   `json:"callers"`
	Faults     []FaultSnap    `json:"faults"`
	Metrics    MetricsSnap    `json:"metrics"`
	MaxInbox   int            `json:"maxInbox"`  // 当前时刻最大排队
	PeakInbox  int            `json:"peakInbox"` // 截至当前时刻的历史峰值
}

func (e *Engine) PeakInbox() int { return e.peakInbox }

type FaultSnap struct {
	Kind  string `json:"kind"`
	Scope string `json:"scope"`
}

type MetricsSnap struct {
	TotalCalls     int64 `json:"totalCalls"`
	FailedCalls    int64 `json:"failedCalls"`
	EmptyListFail  int64 `json:"emptyListFail"`
	EvictionTotal  int64 `json:"evictionTotal"`
	WrongEvictions int64 `json:"wrongEvictions"`
	SentMsgs       int64 `json:"sentMsgs"`
	DroppedMsgs    int64 `json:"droppedMsgs"`
}

func (e *Engine) takeSnapshot() Snapshot {
	s := Snapshot{Tick: e.tick, Faults: []FaultSnap{}}

	for _, reg := range e.registries {
		ratio, actual, expected := reg.ratioForSnapshot(e.tick)
		var direct int64
		for _, b := range reg.buckets {
			direct += b
		}
		evicted, known := 0, 0
		for _, rec := range reg.instances {
			known++
			if rec.evicted {
				evicted++
			}
		}
		s.Registries = append(s.Registries, RegistrySnap{
			ID: reg.ep.ID, Protected: reg.protected, Degraded: reg.degraded,
			Reason: reg.lastReason, Ratio: ratio, Actual: actual, Expected: expected,
			DirectHB: direct, Known: known, Evicted: evicted, Inbox: len(reg.inbox),
		})
		if len(reg.inbox) > s.MaxInbox {
			s.MaxInbox = len(reg.inbox)
		}
	}

	for _, in := range e.instances {
		age := e.tick - in.lastHBAck
		s.Instances = append(s.Instances, InstanceSnap{
			ID: in.ep.ID, Service: in.service, DC: in.ep.DC, SW: in.ep.SW,
			Alive: in.alive, Registered: in.registered, LastHBAge: age,
		})
	}

	for _, c := range e.callers {
		cs := CallerSnap{
			ID: c.ep.ID, DC: c.ep.DC, Service: c.service, Cached: len(c.cache),
			Reachable: c.pullOK, EmptyRejected: c.EmptyRejected,
			Unreachable: c.UnreachableTicks,
			Breakers:    map[string]int{},
		}
		for id, b := range c.breakers {
			cs.Breakers[id] = int(b.State)
		}
		s.Callers = append(s.Callers, cs)
	}

	for _, f := range e.net.ActiveFaults() {
		s.Faults = append(s.Faults, FaultSnap{Kind: string(f.Kind), Scope: f.Scope})
	}

	sent, dropped := e.net.Stats()
	s.Metrics = MetricsSnap{
		TotalCalls: e.TotalCalls, FailedCalls: e.FailedCalls,
		EmptyListFail: e.EmptyListFail, EvictionTotal: e.EvictionTotal,
		WrongEvictions: e.WrongEvictions, SentMsgs: sent, DroppedMsgs: dropped,
	}
	return s
}
