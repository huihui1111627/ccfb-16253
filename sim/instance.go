package sim

// Instance 服务实例。注册与心跳都 gossip 给全部注册中心节点:
// 某个节点自己网络差时,其它节点仍能完整看到该实例,不会被单个节点带偏。
type Instance struct {
	ep        Endpoint
	service   string
	alive     bool
	deathTick int64

	registered bool
	nextRegAt  int64
	nextHBAt   int64
	backoff    int64
	lastHBAck  int64

	regSent int64
	hbSent  int64
}

func NewInstance(ep Endpoint, service string, firstRegAt int64) *Instance {
	return &Instance{
		ep:        ep,
		service:   service,
		alive:     true,
		nextRegAt: firstRegAt,
	}
}

func (in *Instance) Endpoint() Endpoint { return in.ep }
func (in *Instance) Alive() bool        { return in.alive }

// Kill 实例真正宕机:不再发任何消息
func (in *Instance) Kill(tick int64) {
	in.alive = false
	in.deathTick = tick
}

func (in *Instance) Tick(tick int64, e *Engine) {
	if !in.alive {
		return
	}
	switch {
	case !in.registered && tick >= in.nextRegAt:
		// 注册(网络恢复后会自然形成补注册风暴,由注册中心排队限流)
		in.regSent++
		e.BroadcastToRegistries(Message{
			Type: MsgRegister, From: in.ep, SentTick: tick,
			Payload: RegisterPayload{Addr: in.addr()},
		})
		in.backoff = min2(in.backoff*2+2, 30)
		in.nextRegAt = tick + in.backoff
	case in.registered && tick >= in.nextHBAt:
		in.hbSent++
		e.BroadcastToRegistries(Message{
			Type: MsgHeartbeat, From: in.ep, SentTick: tick,
			Payload: HeartbeatPayload{InstanceID: in.ep.ID},
		})
		in.nextHBAt = tick + HBInterval
	}
	// 长时间收不到心跳 ack,认为心跳通道中断过;恢复后第一时间补注册,
	// 大批实例同时恢复时形成注册风暴,压测注册中心的排队限流能力
	if in.registered && tick-in.lastHBAck > 30 {
		in.registered = false
		in.backoff = 0
		in.nextRegAt = tick + 1
	}
}

func (in *Instance) Handle(msg Message, tick int64) {
	if !in.alive {
		return
	}
	switch msg.Type {
	case MsgRegisterAck:
		in.registered = true
		in.backoff = 0
		in.lastHBAck = tick
		in.nextHBAt = tick + HBInterval
	case MsgHeartbeatAck:
		p := msg.Payload.(HBResultPayload)
		// 心跳通道从故障中恢复:立即补注册/补心跳,大批实例同时恢复时
		// 会在注册中心形成真实的突发风暴(由节点排队限流扛住)
		if tick-in.lastHBAck > 30 {
			in.registered = false
			in.backoff = 0
			in.nextRegAt = tick + 1
			in.lastHBAck = tick
			return
		}
		in.lastHBAck = tick
		if p.Unknown {
			// 注册中心已经不认识我(被摘过),重新注册
			in.registered = false
			in.nextRegAt = tick + 1
		}
	}
}

func (in *Instance) addr() InstanceAddr {
	return InstanceAddr{ID: in.ep.ID, Service: in.service, DC: in.ep.DC, SW: in.ep.SW}
}

func min2(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
