package sim

// Report 一次演练的汇总结果
type Report struct {
	ScenarioID   string
	ScenarioName string
	Mode         Mode
	Duration     int64

	TotalCalls     int64
	FailedCalls    int64
	EmptyListFail  int64
	EvictionTotal  int64
	WrongEvictions int64

	// 每个真实宕机实例的屏蔽耗时(tick,100ms)
	DeadMask map[string]int64
	// 自我保护进入/退出记录
	ProtectionTimeline []ProtectionEvent
	// 摘要是从快照序列重建的
	Snapshots  []Snapshot
	PeakInbox  int // 注册中心恢复突发期间出现过的最大排队
	PeakAtTick int64
}

// ProtectionEvent 自我保护状态变化
type ProtectionEvent struct {
	Tick   int64
	NodeID string
	Enter  bool
	Reason string
	Ratio  float64
}

func Run(sc Scenario, mode Mode, seed int64) *Report {
	e := NewEngine(mode, seed)
	e.Run(sc, sc.Duration)

	r := &Report{
		ScenarioID: sc.ID, ScenarioName: sc.Name, Mode: mode, Duration: sc.Duration,
		TotalCalls: e.TotalCalls, FailedCalls: e.FailedCalls,
		EmptyListFail: e.EmptyListFail, EvictionTotal: e.EvictionTotal,
		WrongEvictions: e.WrongEvictions, DeadMask: e.DeadMaskTicks(),
		Snapshots:          e.snapshots,
		ProtectionTimeline: []ProtectionEvent{},
	}
	r.ProtectionTimeline = buildProtectionTimeline(e.snapshots)
	r.PeakInbox = e.peakInbox
	for _, s := range e.snapshots {
		if s.MaxInbox == r.PeakInbox {
			r.PeakAtTick = s.Tick
		}
	}
	return r
}

func buildProtectionTimeline(snaps []Snapshot) []ProtectionEvent {
	prev := map[string]bool{}
	var out []ProtectionEvent
	for _, s := range snaps {
		for _, reg := range s.Registries {
			was := prev[reg.ID]
			if reg.Protected != was {
				prev[reg.ID] = reg.Protected
				out = append(out, ProtectionEvent{
					Tick: s.Tick, NodeID: reg.ID, Enter: reg.Protected,
					Reason: reg.Reason, Ratio: reg.Ratio,
				})
			}
		}
	}
	return out
}

// AvgDeadMaskSeconds 真挂实例平均屏蔽耗时(秒)
func (r *Report) AvgDeadMaskSeconds() float64 {
	if len(r.DeadMask) == 0 {
		return -1
	}
	var sum int64
	for _, t := range r.DeadMask {
		sum += t
	}
	return float64(sum) / float64(len(r.DeadMask)) / 10.0
}

func (r *Report) FailRate() float64 {
	if r.TotalCalls == 0 {
		return 0
	}
	return float64(r.FailedCalls) / float64(r.TotalCalls)
}

// CompareRow 两种模式的对比行
type CompareRow struct {
	ScenarioName  string
	NewFailed     int64
	OldFailed     int64
	NewWrongEvict int64
	OldWrongEvict int64
	NewMaskSec    float64
	OldMaskSec    float64
	NewEmptyList  int64
	OldEmptyList  int64
}

func CompareAll(seed int64) ([]CompareRow, []*Report) {
	var rows []CompareRow
	var all []*Report
	for _, sc := range AllScenarios() {
		nr := Run(sc, ModeProtected, seed)
		or := Run(sc, ModeLegacy, seed)
		all = append(all, nr, or)
		rows = append(rows, CompareRow{
			ScenarioName: sc.Name,
			NewFailed:    nr.FailedCalls, OldFailed: or.FailedCalls,
			NewWrongEvict: nr.WrongEvictions, OldWrongEvict: or.WrongEvictions,
			NewMaskSec: nr.AvgDeadMaskSeconds(), OldMaskSec: or.AvgDeadMaskSeconds(),
			NewEmptyList: nr.EmptyListFail, OldEmptyList: or.EmptyListFail,
		})
	}
	return rows, all
}

// CompareRow 两种模式的对比行
