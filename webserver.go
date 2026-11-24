package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"sync"

	"registrysim/sim"
)

//go:embed web/index.html
var webFS embed.FS

type runCache struct {
	mu sync.Mutex
	m  map[string]*sim.Report
}

func newRunCache() *runCache {
	return &runCache{m: make(map[string]*sim.Report)}
}

func (c *runCache) get(scenarioID string, mode sim.Mode) (*sim.Report, error) {
	key := fmt.Sprintf("%s-%d", scenarioID, mode)
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.m[key]; ok {
		return r, nil
	}
	sc, ok := sim.FindScenario(scenarioID)
	if !ok {
		return nil, fmt.Errorf("未知剧本: %s", scenarioID)
	}
	r := sim.Run(sc, mode, seed)
	c.m[key] = r
	return r, nil
}

func runWeb(args []string) {
	flags := flag.NewFlagSet("web", flag.ExitOnError)
	addr := flags.String("addr", ":8080", "监听地址")
	_ = flags.Parse(args)

	cache := newRunCache()
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/scenarios", func(w http.ResponseWriter, r *http.Request) {
		type scOut struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			Description string `json:"description"`
			Duration    int64  `json:"duration"`
		}
		out := make([]scOut, 0)
		for _, sc := range sim.AllScenarios() {
			out = append(out, scOut{sc.ID, sc.Name, sc.Description, sc.Duration})
		}
		writeJSON(w, out)
	})

	mux.HandleFunc("GET /api/run", func(w http.ResponseWriter, req *http.Request) {
		scID := req.URL.Query().Get("scenario")
		mode := sim.ModeProtected
		if req.URL.Query().Get("mode") == "legacy" {
			mode = sim.ModeLegacy
		}
		r, err := cache.get(scID, mode)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, r)
	})

	mux.HandleFunc("GET /api/compare", func(w http.ResponseWriter, r *http.Request) {
		rows, _ := sim.CompareAll(seed)
		writeJSON(w, rows)
	})

	// embed 的路径带 web/ 前缀,用 fs.Sub 把 web 目录作为静态文件根
	staticFS, err := fs.Sub(webFS, "web")
	if err != nil {
		fmt.Println("静态资源初始化失败:", err)
		return
	}
	mux.Handle("GET /", http.FileServerFS(staticFS))

	fmt.Printf("注册中心模拟可视化页面: http://localhost%s\n", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Println("服务启动失败:", err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
