package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"registrysim/sim"
)

const seed = 42

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "list":
		for _, sc := range sim.AllScenarios() {
			fmt.Printf("%-14s %s\n", sc.ID, sc.Name)
		}
	case "run":
		fs := flag.NewFlagSet("run", flag.ExitOnError)
		modeFlag := fs.String("mode", "protected", "protected | legacy")
		jsonFlag := fs.Bool("json", false, "输出 JSON 报告")
		_ = fs.Parse(os.Args[2:])
		args := fs.Args()
		if len(args) != 1 {
			fmt.Println("用法: registrysim run [-mode protected|legacy] <scenario>")
			os.Exit(1)
		}
		sc, ok := sim.FindScenario(args[0])
		if !ok {
			fmt.Println("未知剧本:", args[0])
			os.Exit(1)
		}
		mode := sim.ModeProtected
		if *modeFlag == "legacy" {
			mode = sim.ModeLegacy
		}
		r := sim.Run(sc, mode, seed)
		if *jsonFlag {
			enc := json.NewEncoder(os.Stdout)
			_ = enc.Encode(r)
			return
		}
		printReport(r)
	case "compare":
		rows, _ := sim.CompareAll(seed)
		printCompare(rows)
	case "web":
		runWeb(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`注册中心自我保护模拟器

用法:
  registrysim list                    列出全部演练剧本
  registrysim run [-mode protected|legacy] [-json] <scenario>
                                      运行单个剧本
  registrysim compare                 五个剧本两种模式全量对比
  registrysim web [-addr :8080]       启动可视化页面`)
}

func printReport(r *sim.Report) {
	fmt.Println("================================================================")
	fmt.Printf("剧本: %s    模式: %s\n", r.ScenarioName, r.Mode)
	fmt.Println("================================================================")
	fmt.Printf("总调用请求:        %d\n", r.TotalCalls)
	fmt.Printf("失败请求:          %d (失败率 %.2f%%)\n", r.FailedCalls, r.FailRate()*100)
	fmt.Printf("因空列表失败:      %d\n", r.EmptyListFail)
	fmt.Printf("摘除实例次数:      %d\n", r.EvictionTotal)
	fmt.Printf("误摘活着的实例:    %d\n", r.WrongEvictions)
	fmt.Printf("节点恢复最大排队:  %d 条(峰值时刻 %.1fs,每 tick 处理上限 %d)\n",
		r.PeakInbox, float64(r.PeakAtTick)/10, sim.InboxCapPerTick)
	if m := r.AvgDeadMaskSeconds(); m >= 0 {
		fmt.Printf("真挂实例平均屏蔽耗时: %.1fs\n", m)
	}
	if len(r.ProtectionTimeline) > 0 {
		fmt.Println("\n自我保护时间线:")
		for _, p := range r.ProtectionTimeline {
			action := "进入"
			if !p.Enter {
				action = "退出"
			}
			fmt.Printf("  %6.1fs %s %s: %s\n", float64(p.Tick)/10, p.NodeID, action, p.Reason)
		}
	} else {
		fmt.Println("\n自我保护: 全程未触发")
	}
}

func printCompare(rows []sim.CompareRow) {
	fmt.Println("五个剧本 × 两种模式 对比(1 tick=100ms,总时长 120s,3000 实例)")
	fmt.Println("================================================================================================================")
	fmt.Printf("%-18s | %10s %10s | %8s %8s | %10s %10s | %10s %10s\n",
		"剧本", "新-失败", "旧-失败", "新-误摘", "旧-误摘",
		"新-屏蔽秒", "旧-屏蔽秒", "新-空列表", "旧-空列表")
	fmt.Println("----------------------------------------------------------------------------------------------------------------")
	for _, r := range rows {
		nm, om := fmt.Sprintf("%.1f", r.NewMaskSec), fmt.Sprintf("%.1f", r.OldMaskSec)
		if r.NewMaskSec < 0 {
			nm = "-"
		}
		if r.OldMaskSec < 0 {
			om = "-"
		}
		fmt.Printf("%-18s | %10d %10d | %8d %8d | %10s %10s | %10d %10d\n",
			r.ScenarioName,
			r.NewFailed, r.OldFailed,
			r.NewWrongEvict, r.OldWrongEvict,
			nm, om,
			r.NewEmptyList, r.OldEmptyList)
	}
}
