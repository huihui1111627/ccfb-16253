package sim

import (
	"math/rand"
)

// Engine 模拟引擎:逻辑时钟驱动,所有组件按 tick 推进,消息经网络投递
type Engine struct {
	mode Mode
	rng  *rand.Rand
	net  *Network

	dcs        []string
	switches   []string
	registries []*Registry
	regEps     []Endpoint
	instances  []*Instance
	instByID   map[string]*Instance
	callers    []*Caller

	events  []ScheduledEvent
	cursor  int
	tick    int64
	pending []Message

	// 指标
	TotalCalls     int64
	FailedCalls    int64
	EmptyListFail  int64 // 因手里列表为空导致的失败
	EvictionTotal  int64 // 摘除事件条数(按节点计,一个实例被 3 节点摘算 3 次)
	WrongEvictions int64 // 被误摘的活着的实例数(按实例去重)
	wrongEvictSet  map[string]bool
	peakInbox      int // 注册中心节点出现过的最大排队长度(恢复突发背压观测)
	EvictionEvents []EvictionRecord
	KillEvents     []KillRecord
	emptyPushFault bool

	snapshots []Snapshot
}

func NewEngine(mode Mode, seed int64) *Engine {
	rng := rand.New(rand.NewSource(seed))
	return &Engine{
		mode:          mode,
		rng:           rng,
		net:           NewNetwork(rng),
		instByID:      make(map[string]*Instance),
		wrongEvictSet: make(map[string]bool),
	}
}

func (e *Engine) Tick() int64                   { return e.tick }
func (e *Engine) RegistryEndpoints() []Endpoint { return e.regEps }
func (e *Engine) Snapshots() []Snapshot         { return e.snapshots }
func (e *Engine) Mode() Mode                    { return e.mode }
func (e *Engine) SetEmptyPushFault(b bool)      { e.emptyPushFault = b }

// FindInstance 按服务与机房挑选一个实例ID,供剧本精准选宕机目标
func (e *Engine) FindInstance(service, dc string) (string, bool) {
	for _, in := range e.instances {
		if in.service == service && (dc == "" || in.ep.DC == dc) {
			return in.ep.ID, true
		}
	}
	return "", false
}

// Send 发送一条消息,网络按故障规则决定是否投递
func (e *Engine) Send(m Message) {
	e.net.sent++
	if !e.net.Reachable(m) {
		e.net.dropped++
		return
	}
	e.pending = append(e.pending, m)
}

// BroadcastToRegistries 实例把注册/心跳 gossip 给全部注册中心节点
func (e *Engine) BroadcastToRegistries(m Message) {
	for _, reg := range e.registries {
		msg := m
		msg.To = reg.ep
		e.Send(msg)
	}
}

func (e *Engine) Run(sc Scenario, totalTicks int64) {
	e.buildTopology(sc)
	e.resolveKillTargets(sc)
	e.events = sc.Events
	for e.tick = 0; e.tick <= totalTicks; e.tick++ {
		e.applyScheduledEvents()
		e.deliverPending()
		for _, in := range e.instances {
			in.Tick(e.tick, e)
		}
		for _, reg := range e.registries {
			reg.Tick(e.tick, e)
			if reg.InboxLen() > e.peakInbox {
				e.peakInbox = reg.InboxLen()
			}
		}
		for _, c := range e.callers {
			c.Tick(e.tick, e)
		}
		if e.tick%SnapshotInterval == 0 {
			snap := e.takeSnapshot()
			snap.PeakInbox = e.peakInbox
			e.snapshots = append(e.snapshots, snap)
		}
	}
}

// resolveKillTargets 剧本里 InstanceID 形如 "user@dc1",按拓扑解析成真实实例ID,
// 保证宕机目标落在指定机房(不依赖实例序号的打散结果)
func (e *Engine) resolveKillTargets(sc Scenario) {
	for i := range sc.Events {
		ev := &sc.Events[i]
		if ev.Op != OpKill || ev.InstanceID == "" {
			continue
		}
		service, dc := ev.InstanceID, ""
		if at := indexByte(ev.InstanceID, '@'); at >= 0 {
			service, dc = ev.InstanceID[:at], ev.InstanceID[at+1:]
		}
		if id, ok := e.FindInstance(service, dc); ok {
			ev.InstanceID = id
		}
	}
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// applyScheduledEvents 故障注入/清除、实例宕机
func (e *Engine) applyScheduledEvents() {
	for e.cursor < len(e.events) && e.events[e.cursor].At <= e.tick {
		ev := e.events[e.cursor]
		e.cursor++
		switch ev.Op {
		case OpFaultOn:
			e.net.AddFault(ev.FaultID, Fault{Kind: ev.Kind, Scope: ev.Scope})
		case OpFaultOff:
			e.net.RemoveFault(ev.FaultID)
		case OpKill:
			if in, ok := e.instByID[ev.InstanceID]; ok && in.Alive() {
				in.Kill(e.tick)
				e.KillEvents = append(e.KillEvents, KillRecord{ID: ev.InstanceID, Tick: e.tick, Service: in.service})
			}
		}
	}
}

// deliverPending 上一 tick 产生的消息本 tick 投递(统一 1 tick 网络延迟)
func (e *Engine) deliverPending() {
	msgs := e.pending
	e.pending = nil
	for _, m := range msgs {
		switch m.To.Type {
		case EPRegistry:
			for _, reg := range e.registries {
				if reg.ep.ID == m.To.ID {
					reg.Enqueue(m)
					break
				}
			}
		case EPInstance:
			if in, ok := e.instByID[m.To.ID]; ok {
				in.Handle(m, e.tick)
			}
		case EPCaller:
			for _, c := range e.callers {
				if c.ep.ID == m.To.ID {
					c.Handle(m, e.tick)
					break
				}
			}
		}
	}
}

// InvokeCall 执行业务调用,返回(是否成功, 是否因网络不通失败)。
// 数据面连通且实例活着才算成功,计入业务请求指标
func (e *Engine) InvokeCall(c *Caller, addr InstanceAddr, tick int64) (bool, bool) {
	in := e.instByID[addr.ID]
	m := Message{Type: MsgCall, From: c.ep, To: in.ep}
	reachable := in != nil && e.net.Reachable(m)
	ok := reachable && in.Alive()
	netFail := reachable == false
	e.RecordCall(c, addr, ok, tick, false)
	return ok, netFail
}

// InvokeProbe 主动健康探测:走同一条数据面判定,但不计入业务请求成败
func (e *Engine) InvokeProbe(c *Caller, addr InstanceAddr, tick int64) (bool, bool) {
	in := e.instByID[addr.ID]
	m := Message{Type: MsgCall, From: c.ep, To: in.ep}
	reachable := in != nil && e.net.Reachable(m)
	return reachable && in.Alive(), !reachable
}

// RequestPull 同步拉取:模拟一次完整 RPC,直接向目标节点要响应,
// 不经过异步消息队列,保证调用方在同一个 tick 内完成多节点并集
func (e *Engine) RequestPull(from, regEP Endpoint, service string, tick int64) *PullPayload {
	for _, reg := range e.registries {
		if reg.ep.ID != regEP.ID {
			continue
		}
		var resp PullPayload
		reg.HandlePullSync(PullPayload{Service: service}, tick, e, &resp)
		return &resp
	}
	return nil
}

func (e *Engine) RecordCall(c *Caller, addr InstanceAddr, ok bool, tick int64, noCandidate bool) {
	e.TotalCalls++
	if !ok {
		e.FailedCalls++
		if noCandidate {
			e.EmptyListFail++
		}
	}
}

// OnEviction 摘除事件记录,并判断是不是误摘(实例实际还活着)
func (e *Engine) OnEviction(id string, addr InstanceAddr, tick int64, inProtection bool) {
	e.EvictionTotal++
	alive := false
	if in, ok := e.instByID[id]; ok {
		alive = in.Alive()
	}
	if alive && !e.wrongEvictSet[id] {
		e.wrongEvictSet[id] = true
		e.WrongEvictions++
	}
	e.EvictionEvents = append(e.EvictionEvents, EvictionRecord{
		InstanceID: id, Tick: tick, DC: addr.DC, SW: addr.SW,
		Alive: alive, InProtection: inProtection,
	})
}

// DeadMaskTicks 真挂实例从宕机到不再被调用的耗时(tick)。
// 优先取调用方首次熔断时间(新模式的主要屏蔽手段);
// 没有熔断记录时再用注册中心摘除时间(旧模式的主要手段)兜底。
func (e *Engine) DeadMaskTicks() map[string]int64 {
	out := make(map[string]int64)
	for _, k := range e.KillEvents {
		maskAt := int64(-1)

		// 1) 调用方熔断:宕机后任何一个调用方对它打开熔断即视为不再被调用
		for _, c := range e.callers {
			b, ok := c.breakers[k.ID]
			if !ok {
				continue
			}
			for _, ev := range b.OpenEvents {
				if ev.Tick >= k.Tick && (maskAt < 0 || ev.Tick < maskAt) {
					maskAt = ev.Tick
				}
			}
		}

		// 2) 注册中心摘除兜底(旧模式没有有效熔断时走这里)。
		// 旧模式可能在实例宕机前就因网络抖动把它摘了,摘除时间早于宕机,按 0 计。
		if maskAt < 0 {
			for _, ev := range e.EvictionEvents {
				if ev.InstanceID == k.ID {
					maskAt = ev.Tick
					break
				}
			}
		}
		if maskAt >= 0 {
			d := maskAt - k.Tick
			if d < 0 {
				d = 0
			}
			out[k.ID] = d
		}
	}
	return out
}
