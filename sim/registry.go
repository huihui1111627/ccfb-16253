package sim

import "strconv"

// instRec 注册中心保存的单实例租约
type instRec struct {
	addr      InstanceAddr
	lastHB    int64
	evicted   bool
	evictedAt int64
}

// Registry 注册中心节点
type Registry struct {
	ep   Endpoint
	mode Mode

	instances map[string]*instRec
	// 10 个桶的滑动窗口,每桶 1s,记录该桶直接收到的心跳条数
	buckets     [WindowTicks / BucketTicks]int64
	bucketStart int64

	// 对端节点摘要:nodeID -> 最新摘要
	peers map[string]*Digest

	protected       bool
	degraded        bool
	entryRun        int
	exitRun         int
	lastProtectTick int64
	lastReason      string

	nextDigestAt int64
	inbox        []Message // 待处理消息队列(突发恢复时限流)
	digestInbox  []Message
}

func NewRegistry(ep Endpoint, mode Mode, tick int64) *Registry {
	return &Registry{
		ep:           ep,
		mode:         mode,
		instances:    make(map[string]*instRec),
		peers:        make(map[string]*Digest),
		bucketStart:  tick,
		nextDigestAt: tick + DigestInterval,
	}
}

func (r *Registry) Endpoint() Endpoint { return r.ep }
func (r *Registry) Protected() bool    { return r.protected }
func (r *Registry) Degraded() bool     { return r.degraded }
func (r *Registry) Reason() string     { return r.lastReason }
func (r *Registry) InboxLen() int      { return len(r.inbox) }

// Enqueue 引擎按消息类型分流,摘要优先处理,普通消息受每 tick 处理上限约束
func (r *Registry) Enqueue(m Message) {
	if m.Type == MsgDigest {
		r.digestInbox = append(r.digestInbox, m)
	} else {
		r.inbox = append(r.inbox, m)
	}
}

func (r *Registry) Tick(tick int64, e *Engine) {
	// 1. 摘要(集群状态同步)永远先处理,不与南北向流量抢配额
	for _, m := range r.digestInbox {
		r.handleDigest(m)
	}
	r.digestInbox = r.digestInbox[:0]

	// 2. 普通消息按配额处理,多余的留到下一个 tick —— 恢复风暴下的背压
	n := InboxCapPerTick
	if n > len(r.inbox) {
		n = len(r.inbox)
	}
	batch := r.inbox[:n]
	r.inbox = r.inbox[n:]
	for _, m := range batch {
		r.handleNormal(m, tick, e)
	}

	// 3. 心跳窗口滚动
	for tick-r.bucketStart >= BucketTicks {
		copy(r.buckets[:], r.buckets[1:])
		r.buckets[len(r.buckets)-1] = 0
		r.bucketStart += BucketTicks
	}

	// 4. 向其它节点广播自己的摘要
	if tick >= r.nextDigestAt {
		r.broadcastDigest(tick, e)
		r.nextDigestAt = tick + DigestInterval
	}

	// 5. 自我保护判定 + 摘除扫描
	if r.mode == ModeProtected && tick > WarmupTicks {
		r.evaluateProtection(tick)
	}
	r.evictionScan(tick, e)
}

func (r *Registry) handleNormal(m Message, tick int64, e *Engine) {
	switch m.Type {
	case MsgRegister:
		p := m.Payload.(RegisterPayload)
		rec, ok := r.instances[p.Addr.ID]
		if !ok {
			rec = &instRec{addr: p.Addr}
			r.instances[p.Addr.ID] = rec
		}
		rec.evicted = false
		rec.lastHB = tick
		e.Send(Message{Type: MsgRegisterAck, From: r.ep, To: m.From, SentTick: tick})
	case MsgHeartbeat:
		p := m.Payload.(HeartbeatPayload)
		r.buckets[len(r.buckets)-1]++
		rec, ok := r.instances[p.InstanceID]
		if !ok {
			// 未注册过,让客户端走注册流程
			e.Send(Message{Type: MsgHeartbeatAck, From: r.ep, To: m.From, SentTick: tick,
				Payload: HBResultPayload{Unknown: true}})
			return
		}
		rec.lastHB = tick
		if rec.evicted {
			// 摘除后实例补心跳回来:重新接纳,客户端也会补一次注册
			rec.evicted = false
			rec.evictedAt = 0
		}
		e.Send(Message{Type: MsgHeartbeatAck, From: r.ep, To: m.From, SentTick: tick,
			Payload: HBResultPayload{Unknown: false}})
	case MsgPull:
		r.handlePull(m, tick, e)
	}
}

func (r *Registry) handleDigest(m Message) {
	d := m.Payload.(Digest)
	r.peers[d.NodeID] = &d
}

func (r *Registry) broadcastDigest(tick int64, e *Engine) {
	lease := make(map[string]LeaseInfo, len(r.instances))
	var actual int64
	for _, b := range r.buckets {
		actual += b
	}
	for id, rec := range r.instances {
		lease[id] = LeaseInfo{LastHB: rec.lastHB, DC: rec.addr.DC, SW: rec.addr.SW}
	}
	d := Digest{
		NodeID: r.ep.ID, Tick: tick, Protected: r.protected, Degraded: r.degraded,
		DirectActual: actual, LeaseHB: lease,
	}
	for _, reg := range e.RegistryEndpoints() {
		if reg.ID == r.ep.ID {
			continue
		}
		e.Send(Message{Type: MsgDigest, From: r.ep, To: reg, SentTick: tick, Payload: d})
	}
}

// clusterView 合并全部节点的租约视角。
// 任一活跃节点近期还看到某实例心跳,集群就认为它活着 ——
// 这是"某节点自己网络差,别的节点不能被它带着误判"的关键。
type clusterView struct {
	expected      int64 // 集群已知实例数(去重)
	actual        int64 // 最近一个窗口内仍有心跳的实例数(去重)
	missing       []instRec
	concentration float64 // 丢失实例集中在同一机房/交换机的最大占比
}

func (r *Registry) clusterView(tick int64) clusterView {
	// 自己的租约 + 所有未过期对端租约,逐实例取最新心跳
	merged := make(map[string]LeaseInfo)
	for id, rec := range r.instances {
		merged[id] = LeaseInfo{LastHB: rec.lastHB, DC: rec.addr.DC, SW: rec.addr.SW}
	}
	for _, d := range r.peers {
		if tick-d.Tick > LeaseTTL {
			continue
		}
		for id, li := range d.LeaseHB {
			if cur, ok := merged[id]; !ok || li.LastHB > cur.LastHB {
				merged[id] = li
			}
		}
	}
	cv := clusterView{}
	dcCount := make(map[string]int)
	swCount := make(map[string]int)
	for id, li := range merged {
		// 已摘除的实例仍然计入期望分母:否则摘着摘着比例回升,保护永远进不来。
		// 摘除扫描另有 evicted 标记保证不会重复摘。
		cv.expected++
		// 保护判定窗口(2s)短于摘除超时(3s):大批心跳一丢,保护先于摘除生效
		if tick-li.LastHB <= ProtectWindowTicks {
			cv.actual++
		} else {
			cv.missing = append(cv.missing, instRec{addr: InstanceAddr{ID: id, DC: li.DC, SW: li.SW}})
			dcCount[li.DC]++
			swCount[li.SW]++
		}
	}
	if len(cv.missing) > 0 {
		var maxC int
		for _, c := range dcCount {
			if c > maxC {
				maxC = c
			}
		}
		for _, c := range swCount {
			if c > maxC {
				maxC = c
			}
		}
		cv.concentration = float64(maxC) / float64(len(cv.missing))
	}
	return cv
}

// evaluateProtection 自我保护进入/退出判定
func (r *Registry) evaluateProtection(tick int64) {
	cv := r.clusterView(tick)
	if cv.expected == 0 {
		return
	}
	ratio := float64(cv.actual) / float64(cv.expected)
	missingCount := len(cv.missing)

	// 本节点降级判定:集群整体是好的,但我自己直接收到的心跳很少
	// (典型场景:该节点自己的网络在丢包)。降级后不参与摘除,信任集群视图。
	var direct int64
	for _, b := range r.buckets {
		direct += b
	}
	ownRatio := float64(direct) / float64(cv.expected)
	r.degraded = ownRatio < 0.5 && ratio >= EntryRatio

	// 各节点基于同一份集群合并视图独立判定,只靠进入/退出迟滞保持稳定,
	// 不做节点间状态跟随 —— 跟随会在集群同时翻转时产生错误的多数派。
	if !r.protected {
		// 进入条件(两条同时满足,且连续 EntryRuns 个 tick):
		//  A. 心跳比例跌破阈值 —— 说明不是零星个例;
		//  B. 丢失要么量大(>=10% 实例),要么高度集中在同一机房/交换机。
		//  单个实例真挂:ratio 几乎不变,永远进不了保护。
		condA := ratio < EntryRatio
		condB := missingCount >= int(float64(cv.expected)*0.1) ||
			(missingCount >= 3 && cv.concentration >= ConcThreshold)
		if condA && condB {
			r.entryRun++
		} else {
			r.entryRun = 0
		}
		if r.entryRun >= EntryRuns {
			r.protected = true
			r.lastProtectTick = tick
			r.lastReason = r.describeReason(cv, ratio)
			r.exitRun = 0
			r.entryRun = 0
		}
	} else {
		// 退出条件更严格(迟滞):心跳比例恢复且连续 ExitRuns 个 tick
		if ratio >= ExitRatio {
			r.exitRun++
		} else {
			r.exitRun = 0
		}
		if r.exitRun >= ExitRuns {
			r.protected = false
			r.lastReason = "心跳比例已恢复至 " + pct(ratio) + " 并持续 3s,退出保护"
			r.exitRun = 0
		}
	}
}

func (r *Registry) describeReason(cv clusterView, ratio float64) string {
	if cv.concentration >= ConcThreshold {
		return "心跳比例跌至 " + pct(ratio) + ",且丢失实例 " + pct(cv.concentration) +
			" 集中在同一机房/交换机,疑似局部网络故障,暂停摘除"
	}
	return "心跳比例跌至 " + pct(ratio) + ",短时间大批实例同时超时,疑似注册中心/网络侧故障,暂停摘除"
}

// evictionScan 摘除扫描
func (r *Registry) evictionScan(tick int64, e *Engine) {
	if r.mode == ModeProtected {
		// 双保险:保护标志有进入迟滞(0.5s),而比例异常是即时的。
		// 保护标志置位前的空窗期里,只要集群心跳比例已经异常就先不摘,
		// 保证"保护判定先于摘除"这条时序约束。
		cv := r.clusterView(tick)
		ratio := 1.0
		if cv.expected > 0 {
			ratio = float64(cv.actual) / float64(cv.expected)
		}
		if r.protected || r.degraded || ratio < EntryRatio {
			return
		}
	}
	// 新模式用集群合并视图判定超时:某节点自己收不到心跳,但别的节点还在收到,
	// 就不能摘 —— "别的节点不能被它带着误判"在摘除侧的落点。
	// 旧模式只信本节点,保持一刀切行为。
	var merged map[string]LeaseInfo
	if r.mode == ModeProtected {
		merged = r.mergedLeases(tick)
	}
	for id, rec := range r.instances {
		if rec.evicted {
			continue
		}
		lastHB := rec.lastHB
		if r.mode == ModeProtected {
			if li, ok := merged[id]; ok && li.LastHB > lastHB {
				lastHB = li.LastHB
			}
		}
		if tick-lastHB > HBTimeoutTicks {
			rec.evicted = true
			rec.evictedAt = tick
			e.OnEviction(id, rec.addr, tick, r.protected)
		}
	}
}

// mergedLeases 汇总本节点与所有未过期对端的租约,逐实例取最新心跳
func (r *Registry) mergedLeases(tick int64) map[string]LeaseInfo {
	merged := make(map[string]LeaseInfo, len(r.instances))
	for id, rec := range r.instances {
		merged[id] = LeaseInfo{LastHB: rec.lastHB, DC: rec.addr.DC, SW: rec.addr.SW}
	}
	for _, d := range r.peers {
		if tick-d.Tick > LeaseTTL {
			continue
		}
		for id, li := range d.LeaseHB {
			if cur, ok := merged[id]; !ok || li.LastHB > cur.LastHB {
				merged[id] = li
			}
		}
	}
	return merged
}

func (r *Registry) handlePull(m Message, tick int64, e *Engine) {
	p := m.Payload.(PullPayload)
	var resp PullPayload
	r.HandlePullSync(p, tick, e, &resp)
	e.Send(Message{Type: MsgPullResp, From: r.ep, To: m.From, SentTick: tick, Payload: resp})
}

// HandlePullSync 同步拉取逻辑,供同步 RPC 与异步消息两条路径共用
func (r *Registry) HandlePullSync(p PullPayload, tick int64, e *Engine, out *PullPayload) {
	// 演练用故障:故障节点自己视图严重残缺,拉取一律返回不完整的空响应,
	// 用来稳定验证调用方"拒绝异常空列表"的防线
	if f, ok := e.net.FaultByKind(FaultNodeLossy); ok && f.Scope == r.ep.ID {
		if e.emptyPushFault {
			*out = PullPayload{Service: p.Service, Instances: nil, Incomplete: true}
			return
		}
	}
	var list []InstanceAddr
	for _, rec := range r.instances {
		if rec.evicted || rec.addr.Service != p.Service {
			continue
		}
		list = append(list, rec.addr)
	}
	*out = PullPayload{Service: p.Service, Instances: list, Incomplete: r.degraded}
}

// ratioForSnapshot 给页面展示用的当前心跳比例
func (r *Registry) ratioForSnapshot(tick int64) (float64, int64, int64) {
	cv := r.clusterView(tick)
	if cv.expected == 0 {
		return 1, 0, 0
	}
	return float64(cv.actual) / float64(cv.expected), cv.actual, cv.expected
}

func pct(x float64) string {
	return strconv.FormatFloat(x*100, 'f', 1, 64) + "%"
}
