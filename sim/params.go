package sim

// 所有时间参数以 tick 为单位,1 tick = 100ms 模拟时间
const (
	TickMS = 100

	HBInterval     = 10 // 实例每 1s 发一次心跳
	HBTimeoutTicks = 30 // 3s 收不到心跳视为超时
	WindowTicks    = 100
	BucketTicks    = 10 // 10 个桶,每桶 1s

	EntryRatio    = 0.85 // 期望/实际心跳比例低于 85% 怀疑全局性故障
	ExitRatio     = 0.90 // 恢复到 90% 以上才退出保护(迟滞)
	ConcThreshold = 0.70 // 丢失实例 70% 集中在同一机房/交换机,视为局部故障强信号
	EntryRuns     = 5    // 进入条件连续满足 0.5s,抑制瞬时抖动
	ExitRuns      = 30   // 退出条件连续满足 3s,防止反复横跳
	WarmupTicks   = 100  // 启动注册风暴期不做保护判定

	// 保护判定用短窗口(2s):必须比摘除超时(3s)更快感知异常,否则保护来不及生效
	ProtectWindowTicks = 20

	DigestInterval  = 5   // 注册中心节点每 0.5s 同步一次摘要
	LeaseTTL        = 45  // 对端摘要 4.5s 内有效
	InboxCapPerTick = 300 // 每 tick 最多处理 300 条消息,超出排队(恢复突发限流)

	PullInterval  = 10 // 调用方每 1s 拉一次实例列表
	CallInterval  = 2  // 每 200ms 发起一次真实调用
	ProbeCycle    = 20 // 主动健康探测:每个实例每 2s 被探一次,失败同样喂给熔断器
	CallStartTick = 30 // 启动注册期不发调用,保证基线干净

	BreakerFailThresh = 3  // 连续 3 次失败熔断
	BreakerOpenTicks  = 15 // 熔断打开 1.5s 后半开试探
	BreakerHalfOK     = 2  // 半开连续 2 次成功恢复

	SnapshotInterval = 20 // 每 2s 模拟时间一个快照
)

type BreakerState int

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

func (b BreakerState) String() string {
	switch b {
	case BreakerOpen:
		return "熔断打开"
	case BreakerHalfOpen:
		return "半开试探"
	default:
		return "正常"
	}
}
