package sim

import (
	"math/rand"
)

// FaultKind 故障类型
type FaultKind string

const (
	FaultDC        FaultKind = "dc_down"     // 整个机房分区
	FaultSwitch    FaultKind = "switch_down" // 单台交换机故障(实例的数据面与心跳面全断)
	FaultHBPlane   FaultKind = "hb_flap"     // 心跳平面交换机抖动:心跳/注册流量全丢,数据面(调用)仍通
	FaultNodeLossy FaultKind = "node_lossy"  // 某注册中心节点自己的网络差:收发 90% 丢包
)

// Fault 一个生效中的故障
type Fault struct {
	Kind  FaultKind
	Scope string // 机房名 / 交换机名 / 节点ID
}

// Network 模拟网络。所有消息都经它投递,故障规则在这里统一拦截
type Network struct {
	endpoints map[string]Endpoint
	faults    map[string]Fault // key = faultID
	rand      *rand.Rand
	sent      int64
	dropped   int64
}

func NewNetwork(rng *rand.Rand) *Network {
	return &Network{
		endpoints: make(map[string]Endpoint),
		faults:    make(map[string]Fault),
		rand:      rng,
	}
}

func (n *Network) Add(ep Endpoint) {
	n.endpoints[ep.ID] = ep
}

func (n *Network) AddFault(id string, f Fault) {
	n.faults[id] = f
}

func (n *Network) RemoveFault(id string) {
	delete(n.faults, id)
}

func (n *Network) ActiveFaults() []Fault {
	out := make([]Fault, 0, len(n.faults))
	for _, f := range n.faults {
		out = append(out, f)
	}
	return out
}

func (n *Network) FaultByKind(kind FaultKind) (Fault, bool) {
	for _, f := range n.faults {
		if f.Kind == kind {
			return f, true
		}
	}
	return Fault{}, false
}

// Reachable 判断消息从 from 到 to 是否可达。
// 设计要点:
//   - 注册中心之间走核心骨干,机房/交换机/心跳平面故障都不影响集群同步;
//   - 心跳平面抖动只拦截实例 -> 注册中心方向的注册/心跳,调用流量放行,
//     用来还原"实例都活着、心跳全丢"的真实事故;
//   - 单节点丢包只影响该节点自己的收发,别的节点收不到它的数据,
//     但集群用多节点租约合并,不会被它带着误判。
func (n *Network) Reachable(msg Message) bool {
	from := n.endpoints[msg.From.ID]
	to := n.endpoints[msg.To.ID]

	for _, f := range n.faults {
		switch f.Kind {
		case FaultDC:
			// 整个机房分区:机房内外互不可达(注册中心在核心区,集群互通)
			if from.DC == f.Scope || to.DC == f.Scope {
				return false
			}
		case FaultSwitch:
			// 单交换机故障:该交换机下实例的所有进出流量
			if from.SW == f.Scope || to.SW == f.Scope {
				return false
			}
		case FaultHBPlane:
			// 心跳平面抖动:拦截全部实例 -> 注册中心的注册/心跳。
			// Scope 为具体机房时只拦该机房;为 "core" 时拦截所有机房,
			// 还原"心跳平面核心交换机抖动,实例都活着但心跳全丢"的事故。
			// 拉取放行,这样旧模式下注册中心把"已摘空列表"推给调用方的事故链条
			// 才能完整复现;数据面(调用)也放行 —— 实例其实都活着。
			if msg.Type == MsgRegister || msg.Type == MsgHeartbeat {
				if from.Type == EPInstance || to.Type == EPInstance {
					if f.Scope == "core" || from.DC == f.Scope || to.DC == f.Scope {
						return false
					}
				}
			}
		case FaultNodeLossy:
			// 集群骨干不受影响:只拦截该节点与实例/调用方之间的南北向流量
			if (from.ID == f.Scope || to.ID == f.Scope) &&
				!(from.Type == EPRegistry && to.Type == EPRegistry) {
				if n.rand.Float64() < 0.9 {
					return false
				}
			}
		}
	}
	return true
}

func (n *Network) Stats() (sent, dropped int64) {
	return n.sent, n.dropped
}
