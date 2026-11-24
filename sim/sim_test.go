package sim

import "testing"

// 单个实例真挂:心跳比例几乎不变,自我保护绝不能触发
func TestSingleDeathNoProtection(t *testing.T) {
	sc, _ := FindScenario("single_death")
	r := Run(sc, ModeProtected, 42)
	if len(r.ProtectionTimeline) != 0 {
		t.Fatalf("单个实例真挂不应触发自我保护,实际事件: %+v", r.ProtectionTimeline)
	}
	if r.WrongEvictions != 0 {
		t.Fatalf("单个实例真挂不应有误摘,实际: %d", r.WrongEvictions)
	}
	if r.FailedCalls != 0 {
		t.Fatalf("熔断应屏蔽死实例,业务失败请求应为 0,实际: %d", r.FailedCalls)
	}
	// 死实例应在 5s 内被调用方熔断屏蔽
	if d := r.AvgDeadMaskSeconds(); d < 0 || d > 5 {
		t.Fatalf("死实例屏蔽耗时应在 5s 内,实际: %.1fs", d)
	}
}

// 交换机抖动(事故还原):新模式零误摘零失败,旧模式全摘且推空列表
func TestHBFlapProtection(t *testing.T) {
	sc, _ := FindScenario("hb_flap")
	nr := Run(sc, ModeProtected, 42)
	or := Run(sc, ModeLegacy, 42)

	if nr.WrongEvictions != 0 {
		t.Fatalf("新模式抖动期间不应误摘,实际误摘: %d", nr.WrongEvictions)
	}
	if nr.FailedCalls != 0 || nr.EmptyListFail != 0 {
		t.Fatalf("新模式抖动期间业务不应失败,实际失败: %d 空列表失败: %d",
			nr.FailedCalls, nr.EmptyListFail)
	}
	if len(nr.ProtectionTimeline) == 0 {
		t.Fatal("新模式应进入自我保护")
	}
	entered := false
	for _, p := range nr.ProtectionTimeline {
		if p.Enter {
			entered = true
		}
	}
	if !entered {
		t.Fatal("保护时间线中没有进入事件")
	}
	if or.WrongEvictions < 2000 {
		t.Fatalf("旧模式应大批误摘,实际: %d", or.WrongEvictions)
	}
	if or.EmptyListFail == 0 {
		t.Fatal("旧模式应出现空列表导致的请求失败")
	}
}

// 某注册中心节点自己网络差:不能带着其它节点误判
func TestNodeLossyIsolation(t *testing.T) {
	sc, _ := FindScenario("node_lossy")
	r := Run(sc, ModeProtected, 42)
	if r.WrongEvictions != 0 {
		t.Fatalf("单节点丢包不应造成任何误摘,实际: %d", r.WrongEvictions)
	}
	if r.FailedCalls != 0 {
		t.Fatalf("集群其余节点视图完整,业务不应失败,实际: %d", r.FailedCalls)
	}
	// 故障节点返回异常空响应,调用方必须拒绝并保留缓存
	rejected := int64(0)
	for _, s := range r.Snapshots {
		for _, c := range s.Callers {
			if c.EmptyRejected > rejected {
				rejected = c.EmptyRejected
			}
		}
	}
	if rejected == 0 {
		t.Fatal("调用方应拒绝故障节点的异常空响应")
	}
}

// 真挂与抖动同时发生:保护不摘活实例,熔断仍能屏蔽真挂实例
func TestDeathAndFlap(t *testing.T) {
	sc, _ := FindScenario("death_and_flap")
	r := Run(sc, ModeProtected, 42)
	if r.WrongEvictions != 0 {
		t.Fatalf("不应误摘活着的实例,实际: %d", r.WrongEvictions)
	}
	if len(r.DeadMask) != 1 {
		t.Fatalf("应有一个死实例屏蔽记录,实际: %+v", r.DeadMask)
	}
	for id, d := range r.DeadMask {
		if d > 80 {
			t.Fatalf("死实例 %s 屏蔽耗时 %.1fs 过长", id, float64(d)/10)
		}
	}
}

// 机房分区:丢失集中在同一机房,应进入保护且零误摘
func TestDCPartitionProtection(t *testing.T) {
	sc, _ := FindScenario("dc_partition")
	r := Run(sc, ModeProtected, 42)
	if r.WrongEvictions != 0 {
		t.Fatalf("机房分区不应误摘,实际: %d", r.WrongEvictions)
	}
	entered := false
	for _, p := range r.ProtectionTimeline {
		if p.Enter {
			entered = true
		}
	}
	if !entered {
		t.Fatal("丢失集中在同一机房时应进入自我保护")
	}
}
