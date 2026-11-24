package sim

// OpenEvent 一次熔断打开记录。NetworkFail=true 表示因网络分区/抖动打开,
// 不能算到"真实例宕机屏蔽耗时"头上
type OpenEvent struct {
	Tick        int64
	NetworkFail bool
}

// Breaker 调用方对单个实例的失败熔断器
type Breaker struct {
	State        BreakerState
	ConsecFail   int
	ConsecHalfOK int
	OpenedAt     int64
	FailCount    int64
	OpenEvents   []OpenEvent // 每次进入熔断打开的 tick 与原因
}

// Caller 调用方客户端。手里缓存实例列表,注册中心整体不可达时用旧缓存兜底
type Caller struct {
	ep      Endpoint
	service string
	mode    Mode

	cache      []InstanceAddr
	nextPullAt int64
	pullOK     bool
	breakers   map[string]*Breaker
	pickCursor int

	EmptyRejected    int64 // 拒绝异常空列表的次数
	UnreachableTicks int64 // 注册中心整体不可达持续 tick 数
	ProbeFail        int64 // 主动健康探测失败次数(不计入业务请求失败)
}

func NewCaller(ep Endpoint, service string, mode Mode, startTick int64) *Caller {
	return &Caller{
		ep:         ep,
		service:    service,
		mode:       mode,
		nextPullAt: startTick + 5,
		breakers:   make(map[string]*Breaker),
	}
}

func (c *Caller) Endpoint() Endpoint { return c.ep }
func (c *Caller) Breaker(id string) *Breaker {
	b, ok := c.breakers[id]
	if !ok {
		b = &Breaker{}
		c.breakers[id] = b
	}
	return b
}

func (c *Caller) Handle(m Message, tick int64) {
	if m.Type != MsgPullResp {
		return
	}
	p := m.Payload.(PullPayload)
	switch c.mode {
	case ModeLegacy:
		// 旧模式:给什么信什么,空列表直接生效 —— 事故全站调不通的根因
		c.cache = p.Instances
		c.pullOK = true
	default:
		// 新模式两道防线:
		//  1. 节点自己标记了视图不完整(降级),拒绝;
		//  2. 之前明明有实例,突然推一个空列表,拒绝并保留旧缓存。
		if p.Incomplete {
			c.EmptyRejected++
			return
		}
		if len(p.Instances) == 0 && len(c.cache) > 0 {
			c.EmptyRejected++
			return
		}
		c.cache = p.Instances
		c.pullOK = true
	}
}

func (c *Caller) Tick(tick int64, e *Engine) {
	if tick >= c.nextPullAt {
		c.pull(tick, e)
	}
	if tick >= CallStartTick && tick%CallInterval == 0 {
		c.makeCall(tick, e)
	}
	// 主动健康探测:为避免 250 个实例按顺序轮转导致死实例发现慢,
	// 每个实例按 (位置 + 抖动) 决定探测时刻,每个周期打散到全部 tick 上;
	// 已出现连续失败、尚未熔断的实例每 10 tick 加探一次,加快熔断确认。
	if tick >= CallStartTick && len(c.cache) > 0 {
		for pos, addr := range c.cache {
			b := c.Breaker(addr.ID)
			if b.State == BreakerOpen {
				continue
			}
			slot := (pos*7 + int(addr.ID[len(addr.ID)-1])) % ProbeCycle
			normal := tick%ProbeCycle == int64(slot)
			fast := b.ConsecFail > 0 && tick%10 == int64(slot%10)
			if !normal && !fast {
				continue
			}
			ok, netFail := e.InvokeProbe(c, addr, tick)
			if !ok {
				c.ProbeFail++
			}
			c.feedBreaker(addr.ID, ok, tick, netFail)
		}
	}
	// 熔断状态推进:打开时间到 -> 半开试探
	for _, b := range c.breakers {
		if b.State == BreakerOpen && tick-b.OpenedAt >= BreakerOpenTicks {
			b.State = BreakerHalfOpen
			b.ConsecHalfOK = 0
		}
	}
}

func (c *Caller) pull(tick int64, e *Engine) {
	regs := e.RegistryEndpoints()
	// 一个轮询周期内拉全部注册中心,结果取并集:
	// 任一节点仍认为某实例活着,调用方就保留它 —— 单节点视图偏差不影响调用方;
	// 旧模式下只有全部节点都摘光,并集才会变空,此时空列表生效(事故链条复现)。
	var union []InstanceAddr
	seen := map[string]bool{}
	anyReachable := false
	anyIncomplete := false
	for _, reg := range regs {
		probe := Message{Type: MsgPull, From: c.ep, To: reg}
		if !e.net.Reachable(probe) {
			continue
		}
		anyReachable = true
		resp := e.RequestPull(c.ep, reg, c.service, tick)
		if resp == nil {
			continue
		}
		if resp.Incomplete {
			anyIncomplete = true
			continue
		}
		for _, a := range resp.Instances {
			if !seen[a.ID] {
				seen[a.ID] = true
				union = append(union, a)
			}
		}
	}
	c.nextPullAt = tick + PullInterval
	if !anyReachable {
		// 所有注册中心都不可达:保留旧缓存继续调用,标记为兜底状态
		c.UnreachableTicks++
		c.pullOK = false
		c.nextPullAt = tick + 2
		return
	}
	c.applyPullResult(union, anyIncomplete, tick)
}

// applyPullResult 按模式决定是否接受本轮拉取结果
func (c *Caller) applyPullResult(list []InstanceAddr, incomplete bool, tick int64) {
	switch c.mode {
	case ModeLegacy:
		// 旧模式:给什么信什么,空列表直接生效 —— 事故全站调不通的根因
		c.cache = list
		c.pullOK = true
	default:
		// 新模式两道防线:
		//  1. 节点自己标记了视图不完整(降级),拒绝;
		//  2. 之前明明有实例,突然推一个空列表,拒绝并保留旧缓存。
		if incomplete {
			c.EmptyRejected++
			return
		}
		if len(list) == 0 && len(c.cache) > 0 {
			c.EmptyRejected++
			return
		}
		c.cache = list
		c.pullOK = true
	}
}

func (c *Caller) makeCall(tick int64, e *Engine) {
	if len(c.cache) == 0 {
		// 手里一个实例都没有:这次请求必然失败
		e.RecordCall(c, InstanceAddr{}, false, tick, true)
		return
	}
	// 轮询挑选一个没有处于熔断打开状态的实例
	n := len(c.cache)
	for i := 0; i < n; i++ {
		addr := c.cache[(c.pickCursor+i)%n]
		b := c.Breaker(addr.ID)
		if b.State != BreakerOpen {
			c.pickCursor = (c.pickCursor + i + 1) % n
			ok, netFail := e.InvokeCall(c, addr, tick)
			c.feedBreaker(addr.ID, ok, tick, netFail)
			return
		}
	}
	// 全部熔断:无可用实例,请求失败
	e.RecordCall(c, InstanceAddr{}, false, tick, true)
}

// feedNetworkFailure 网络类失败:熔断仍然要打开(请求确实打不过去),
// 但打开事件标记为网络原因,不计入实例真挂的屏蔽耗时
func (c *Caller) feedNetworkFailure(id string, ok bool, tick int64) {
	if ok {
		c.feedBreaker(id, true, tick, true)
		return
	}
	b := c.Breaker(id)
	b.FailCount++
	if b.State == BreakerHalfOpen {
		b.State = BreakerOpen
		b.OpenedAt = tick
		b.OpenEvents = append(b.OpenEvents, OpenEvent{Tick: tick, NetworkFail: true})
		b.ConsecHalfOK = 0
		return
	}
	b.ConsecFail++
	if b.ConsecFail >= BreakerFailThresh {
		b.State = BreakerOpen
		b.OpenedAt = tick
		b.OpenEvents = append(b.OpenEvents, OpenEvent{Tick: tick, NetworkFail: true})
	}
}

// feedBreaker 失败结果喂给熔断器。networkFail 区分"实例没响应"与"网络不通"
func (c *Caller) feedBreaker(id string, ok bool, tick int64, networkFail bool) {
	b := c.Breaker(id)
	switch {
	case ok && b.State == BreakerHalfOpen:
		b.ConsecHalfOK++
		if b.ConsecHalfOK >= BreakerHalfOK {
			b.State = BreakerClosed
			b.ConsecFail = 0
		}
	case ok:
		b.ConsecFail = 0
	default:
		b.FailCount++
		if b.State == BreakerHalfOpen {
			// 半开仍失败:重新打开,再等一个冷却周期
			b.State = BreakerOpen
			b.OpenedAt = tick
			b.OpenEvents = append(b.OpenEvents, OpenEvent{Tick: tick, NetworkFail: networkFail})
			b.ConsecHalfOK = 0
			return
		}
		b.ConsecFail++
		if b.ConsecFail >= BreakerFailThresh {
			b.State = BreakerOpen
			b.OpenedAt = tick
			b.OpenEvents = append(b.OpenEvents, OpenEvent{Tick: tick, NetworkFail: networkFail})
		}
	}
}
