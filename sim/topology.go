package sim

import "fmt"

// buildTopology 构建拓扑:
//
//	3 个机房,每机房 2 台交换机;12 个服务,每服务 250 个实例,共 3000 实例;
//	注册中心 3 节点与 6 个调用方部署在核心区(跨机房)。
func (e *Engine) buildTopology(sc Scenario) {
	e.dcs = []string{"dc1", "dc2", "dc3"}
	perDC := 2
	services := []string{"user", "order", "pay", "inventory",
		"coupon", "message", "search", "recommend",
		"settle", "risk", "gateway", "profile"}
	svcPerService := 250

	// 交换机
	for _, dc := range e.dcs {
		for s := 0; s < perDC; s++ {
			sw := fmt.Sprintf("%s-sw%d", dc, s+1)
			e.switches = append(e.switches, sw)
		}
	}

	// 注册中心 3 节点(核心区)
	for i := 1; i <= 3; i++ {
		ep := Endpoint{ID: fmt.Sprintf("reg%d", i), Type: EPRegistry}
		e.net.Add(ep)
		r := NewRegistry(ep, e.mode, 0)
		e.registries = append(e.registries, r)
		e.regEps = append(e.regEps, ep)
	}

	// 实例:均匀打散到各机房各交换机,注册时间加随机抖动(避免开机注册风暴)
	idx := 0
	for _, svc := range services {
		for k := 0; k < svcPerService; k++ {
			dc := e.dcs[idx%len(e.dcs)]
			sw := fmt.Sprintf("%s-sw%d", dc, (idx/len(e.dcs))%perDC+1)
			id := fmt.Sprintf("%s-inst%03d", svc, k)
			ep := Endpoint{ID: id, Type: EPInstance, DC: dc, SW: sw}
			e.net.Add(ep)
			firstReg := 2 + e.rng.Int63n(50)
			in := NewInstance(ep, svc, firstReg)
			e.instances = append(e.instances, in)
			e.instByID[id] = in
			idx++
		}
	}

	// 调用方:每机房 2 个,各自调一个服务,缓存互不影响
	callerSvcs := []string{"user", "order", "pay", "inventory", "coupon", "message"}
	for i, svc := range callerSvcs {
		dc := e.dcs[i%len(e.dcs)]
		ep := Endpoint{ID: fmt.Sprintf("caller-%d", i+1), Type: EPCaller, DC: dc}
		e.net.Add(ep)
		e.callers = append(e.callers, NewCaller(ep, svc, e.mode, 0))
	}
	e.emptyPushFault = sc.EmptyPush
}

func (e *Engine) Services() []string {
	seen := map[string]bool{}
	var out []string
	for _, in := range e.instances {
		if !seen[in.service] {
			seen[in.service] = true
			out = append(out, in.service)
		}
	}
	return out
}
