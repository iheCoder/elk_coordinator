package http

import (
	"elk_coordinator/data"
	"elk_coordinator/utils"
	"net/http"
	"time"
)

// Router HTTP路由器
type Router struct {
	namespace string
	dataStore data.DataStore
	logger    utils.Logger
}

// NewRouter 创建新的路由器
func NewRouter(namespace string, dataStore data.DataStore, logger utils.Logger) *Router {
	if logger == nil {
		logger = utils.NewDefaultLogger()
	}

	return &Router{
		namespace: namespace,
		dataStore: dataStore,
		logger:    logger,
	}
}

// SetupRoutes 设置路由
func (rt *Router) SetupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// API路由
	mux.HandleFunc("/api/v1/retry-failed-partitions",
		RetryFailedPartitionsHandler(rt.namespace, rt.dataStore, rt.logger))
	mux.HandleFunc("/health", rt.healthCheckHandler)

	// 添加中间件
	return rt.withMiddleware(mux)
}

// healthCheckHandler 健康检查处理器
func (rt *Router) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok","service":"elk-coordinator"}`))
}

// withMiddleware 添加中间件
func (rt *Router) withMiddleware(handler http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", rt.loggingMiddleware(rt.corsMiddleware(handler)))
	return mux
}

// loggingMiddleware 日志中间件
func (rt *Router) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 包装ResponseWriter以捕获状态码
		wrappedWriter := &responseWriter{ResponseWriter: w}

		next.ServeHTTP(wrappedWriter, r)

		duration := time.Since(start)
		rt.logger.Infof("HTTP %s %s - %d - %v", r.Method, r.URL.Path, wrappedWriter.statusCode, duration)
	})
}

// corsMiddleware CORS中间件
func (rt *Router) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// responseWriter 包装ResponseWriter以捕获状态码
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.statusCode == 0 {
		rw.statusCode = 200
	}
	return rw.ResponseWriter.Write(b)
}
