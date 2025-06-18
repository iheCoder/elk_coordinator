package http

import (
	"elk_coordinator/data"
	"elk_coordinator/utils"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Router HTTP路由器
type Router struct {
	namespace string
	dataStore data.DataStore
	logger    utils.Logger
	engine    *gin.Engine
}

// NewRouter 创建新的路由器
func NewRouter(namespace string, dataStore data.DataStore, logger utils.Logger) *Router {
	if logger == nil {
		logger = utils.NewDefaultLogger()
	}

	// 设置Gin模式
	gin.SetMode(gin.ReleaseMode)

	engine := gin.New()

	return &Router{
		namespace: namespace,
		dataStore: dataStore,
		logger:    logger,
		engine:    engine,
	}
}

// SetupRoutes 设置路由
func (rt *Router) SetupRoutes() *gin.Engine {
	// 添加中间件
	rt.engine.Use(rt.loggingMiddleware())
	rt.engine.Use(rt.corsMiddleware())
	rt.engine.Use(gin.Recovery())

	// API路由组
	v1 := rt.engine.Group("/api/v1")
	{
		v1.POST("/retry-failed-partitions", rt.retryFailedPartitionsHandler())
	}

	// 健康检查路由
	rt.engine.GET("/health", rt.healthCheckHandler())

	return rt.engine
}

// GetEngine 获取Gin引擎
func (rt *Router) GetEngine() *gin.Engine {
	return rt.engine
}

// healthCheckHandler 健康检查处理器
func (rt *Router) healthCheckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"service": "elk-coordinator",
		})
	}
}

// retryFailedPartitionsHandler 重试失败分区处理器
func (rt *Router) retryFailedPartitionsHandler() gin.HandlerFunc {
	return RetryFailedPartitionsGinHandler(rt.namespace, rt.dataStore, rt.logger)
}

// loggingMiddleware 日志中间件
func (rt *Router) loggingMiddleware() gin.HandlerFunc {
	return gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		rt.logger.Infof("HTTP %s %s - %d - %v",
			param.Method,
			param.Path,
			param.StatusCode,
			param.Latency,
		)
		return ""
	})
}

// corsMiddleware CORS中间件
func (rt *Router) corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusOK)
			return
		}

		c.Next()
	}
}
