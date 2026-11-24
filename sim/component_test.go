package sim

import "testing"

// 调用方防线一:带 Incomplete 标记的异常空响应必须拒绝
func TestCallerRejectsIncompleteEmpty(t *testing.T) {
	c := NewCaller(Endpoint{ID: "c1", Type: EPCaller}, "user", ModeProtected, 0)
	c.cache = []InstanceAddr{{ID: "a"}, {ID: "b"}}
	before := len(c.cache)

	c.Handle(Message{Type: MsgPullResp, Payload: PullPayload{
		Service: "user", Instances: nil, Incomplete: true,
	}}, 10)

	if c.EmptyRejected != 1 {
		t.Fatalf("应计一次拒绝,实际: %d", c.EmptyRejected)
	}
	if len(c.cache) != before {
		t.Fatal("拒绝异常响应后旧缓存必须保留")
	}
}

// 调用方防线二:曾经有实例,突然推裸空列表也要拒绝
func TestCallerRejectsBareEmpty(t *testing.T) {
	c := NewCaller(Endpoint{ID: "c2", Type: EPCaller}, "user", ModeProtected, 0)
	c.cache = []InstanceAddr{{ID: "a"}}

	c.Handle(Message{Type: MsgPullResp, Payload: PullPayload{
		Service: "user", Instances: nil, Incomplete: false,
	}}, 10)

	if c.EmptyRejected != 1 || len(c.cache) != 1 {
		t.Fatalf("裸空列表应被拒绝,拒绝=%d 缓存=%d", c.EmptyRejected, len(c.cache))
	}
}

// 对照:旧模式不设防,空列表直接生效
func TestLegacyCallerAcceptsEmpty(t *testing.T) {
	c := NewCaller(Endpoint{ID: "c3", Type: EPCaller}, "user", ModeLegacy, 0)
	c.cache = []InstanceAddr{{ID: "a"}}

	c.Handle(Message{Type: MsgPullResp, Payload: PullPayload{
		Service: "user", Instances: nil,
	}}, 10)

	if len(c.cache) != 0 {
		t.Fatal("旧模式应接受空列表,这正是事故根因")
	}
}

// 熔断器:连续 3 次失败打开,1.5s 后半开,连续 2 次成功恢复
func TestBreakerLifecycle(t *testing.T) {
	c := NewCaller(Endpoint{ID: "c4", Type: EPCaller}, "user", ModeProtected, 0)
	for i := int64(0); i < 3; i++ {
		c.feedBreaker("a", false, i, false)
	}
	b := c.Breaker("a")
	if b.State != BreakerOpen {
		t.Fatalf("3 次失败后应熔断打开,实际: %v", b.State)
	}
	// 冷却期内保持打开
	c.Tick(10, &Engine{})
	if b.State != BreakerOpen {
		t.Fatal("冷却期内不应变化")
	}
	// 冷却结束后的下一个 tick 进入半开
	c.Tick(BreakerOpenTicks+3, &Engine{})
	if b.State != BreakerHalfOpen {
		t.Fatalf("冷却后应半开,实际: %v", b.State)
	}
	c.feedBreaker("a", true, 20, false)
	c.feedBreaker("a", true, 21, false)
	if b.State != BreakerClosed {
		t.Fatalf("半开连续成功应恢复,实际: %v", b.State)
	}
}

// 集群合并视图:本节点看不到心跳,但对端还在看到,实例不算丢失
func TestClusterViewLeaseMerge(t *testing.T) {
	r := NewRegistry(Endpoint{ID: "reg1", Type: EPRegistry}, ModeProtected, 0)
	r.instances["x"] = &instRec{
		addr:   InstanceAddr{ID: "x", DC: "dc1", SW: "dc1-sw1"},
		lastHB: 0, // 本节点视角早就超时
	}
	// 对端 2 tick 前还看到该实例心跳
	r.peers["reg2"] = &Digest{
		NodeID: "reg2", Tick: 100,
		LeaseHB: map[string]LeaseInfo{
			"x": {LastHB: 98, DC: "dc1", SW: "dc1-sw1"},
		},
	}
	cv := r.clusterView(100)
	if cv.expected != 1 || cv.actual != 1 || len(cv.missing) != 0 {
		t.Fatalf("对端仍见心跳时实例不应丢失: expected=%d actual=%d missing=%d",
			cv.expected, cv.actual, len(cv.missing))
	}
}
