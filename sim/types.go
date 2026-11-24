package sim

// Mode 注册中心的摘除策略模式
type Mode int

const (
	// ModeProtected 新模式:带自我保护,配合调用方失败探测
	ModeProtected Mode = iota
	// ModeLegacy 旧模式:一刀切,心跳超时立即摘除,空列表直接生效
	ModeLegacy
)

func (m Mode) String() string {
	if m == ModeLegacy {
		return "一刀切(旧模式)"
	}
	return "自我保护(新模式)"
}

// EndpointType 端点类型
type EndpointType int

const (
	EPInstance EndpointType = iota
	EPRegistry
	EPCaller
)

// Endpoint 网络端点。注册中心与调用方在核心区,SW 为空;实例挂在某机房某交换机下
type Endpoint struct {
	ID   string
	Type EndpointType
	DC   string // 机房,核心区为空
	SW   string // 交换机,核心区为空
}

// MsgType 消息类型
type MsgType int

const (
	MsgRegister MsgType = iota
	MsgRegisterAck
	MsgHeartbeat
	MsgHeartbeatAck
	MsgPull
	MsgPullResp
	MsgDigest // 注册中心节点之间的状态同步
	MsgCall
)

// Message 模拟网络上的一条消息,按逻辑时钟 tick 投递
type Message struct {
	Type     MsgType
	From     Endpoint
	To       Endpoint
	SentTick int64
	Payload  any
}

// InstanceAddr 实例地址信息,调用方缓存的元素
type InstanceAddr struct {
	ID      string
	Service string
	DC      string
	SW      string
}

type RegisterPayload struct {
	Addr InstanceAddr
}

type HeartbeatPayload struct {
	InstanceID string
}

// HBResultPayload 心跳/注册应答。Unknown=true 表示注册中心已没有该实例,客户端需重新注册
type HBResultPayload struct {
	Unknown bool
}

// LeaseInfo 摘要里携带的单实例租约视角
type LeaseInfo struct {
	LastHB int64
	DC     string
	SW     string
}

// PullPayload 拉取请求与响应共用。响应时 Instances 为空列表表示注册中心返回了空
type PullPayload struct {
	Service    string
	Instances  []InstanceAddr
	Incomplete bool // 节点自身降级,视图不完整
}

// Digest 注册中心节点之间周期性同步的状态摘要
type Digest struct {
	NodeID       string
	Tick         int64
	Protected    bool
	Degraded     bool
	DirectActual int64                // 本节点窗口内直接收到的心跳条数
	LeaseHB      map[string]LeaseInfo // 本节点视角:实例ID -> 租约信息
}

// 剧本事件操作
type EventOp int

const (
	OpFaultOn EventOp = iota
	OpFaultOff
	OpKill
)

// ScheduledEvent 剧本中的一个定时事件
type ScheduledEvent struct {
	At         int64
	Op         EventOp
	Kind       FaultKind
	Scope      string
	FaultID    string
	InstanceID string
}

// Scenario 一个演练剧本
type Scenario struct {
	ID          string
	Name        string
	Description string
	Events      []ScheduledEvent
	Duration    int64
	EmptyPush   bool // 是否注入"异常空响应"故障(验证调用方拒绝空列表)
}
