package sim

// 剧本时间轴:总时长 120s(1200 tick),故障一般在 30s 注入。
// 1 tick = 100ms,所以 200 tick = 20s,与真实事故的"抖动 20 秒"对齐。
func AllScenarios() []Scenario {
	return []Scenario{
		{
			ID:          "single_death",
			Name:        "单个实例真挂",
			Description: "user 服务一个实例在 30s 时真实宕机,不再有任何心跳与响应。验证:新模式下自我保护不应被触发,调用方靠失败熔断快速屏蔽它。",
			Duration:    1200,
			Events: []ScheduledEvent{
				{At: 300, Op: OpKill, InstanceID: "user@"},
			},
		},
		{
			ID:          "dc_partition",
			Name:        "机房网络分区",
			Description: "dc2 整个机房在 30s~70s 被分区,机房内实例与核心区完全隔离。验证:丢失实例高度集中在同一机房,集群进入自我保护,不误摘;分区内调用方用本地缓存兜底。",
			Duration:    1200,
			Events: []ScheduledEvent{
				{At: 300, Op: OpFaultOn, Kind: FaultDC, Scope: "dc2", FaultID: "dc2-partition"},
				{At: 700, Op: OpFaultOff, FaultID: "dc2-partition"},
			},
		},
		{
			ID:          "node_lossy",
			Name:        "注册中心节点网络差",
			Description: "reg2 节点在 30s~70s 自己 90% 丢包,并向调用方返回不完整的空响应。验证:其它节点靠多租约合并不被带偏;reg2 自我降级不参与摘除;调用方拒绝异常空列表。",
			Duration:    1200,
			EmptyPush:   true,
			Events: []ScheduledEvent{
				{At: 300, Op: OpFaultOn, Kind: FaultNodeLossy, Scope: "reg2", FaultID: "reg2-lossy"},
				{At: 700, Op: OpFaultOff, FaultID: "reg2-lossy"},
			},
		},
		{
			ID:          "hb_flap",
			Name:        "交换机抖动(还原事故)",
			Description: "心跳平面核心交换机在 30s~50s 抖动 20 秒:全部实例活着、数据面调用正常,但所有机房的心跳/注册全丢。验证:新模式下零误摘、零列表清空;旧模式把超时实例全部摘除并向调用方推空列表,20 秒抖动演化为全站故障。",
			Duration:    1200,
			Events: []ScheduledEvent{
				{At: 300, Op: OpFaultOn, Kind: FaultHBPlane, Scope: "core", FaultID: "core-flap"},
				{At: 500, Op: OpFaultOff, FaultID: "core-flap"},
			},
		},
		{
			ID:          "death_and_flap",
			Name:        "真挂与抖动同时发生",
			Description: "心跳平面核心交换机抖动 20 秒,期间一个 user 实例真实宕机。验证最棘手的组合:集群因抖动进入保护不摘除,但调用方熔断仍能把真挂实例屏蔽掉,故障恢复后保护正常退出。",
			Duration:    1200,
			Events: []ScheduledEvent{
				{At: 300, Op: OpFaultOn, Kind: FaultHBPlane, Scope: "core", FaultID: "core-flap"},
				{At: 350, Op: OpKill, InstanceID: "user@dc1"},
				{At: 500, Op: OpFaultOff, FaultID: "core-flap"},
			},
		},
	}
}

func FindScenario(id string) (Scenario, bool) {
	for _, sc := range AllScenarios() {
		if sc.ID == id {
			return sc, true
		}
	}
	return Scenario{}, false
}
