package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.metrics.Metrics
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

/** `/healthz` 的结果。[healthy] 决定状态码（200 / 503），[body] 是给人看的正文。 */
data class HealthReport(val healthy: Boolean, val body: String)

/**
 * 观测 HTTP 端点：`/metrics`（Prometheus 文本）与 `/healthz`。
 *
 * ## 为什么用 JDK 自带的 `HttpServer` 而不是 Netty
 *
 * MySQL 协议服务端（`MysqlServer`）刻意不用 Netty 是因为协议有状态、每连接串行；
 * 观测端点连那个理由都没有：两个 GET、几百到几千字节、没有任何连接状态。
 * 为此引入 Netty 会换来一个事件循环线程、一堆缓冲区池和一套完全不同的错误模型
 * （见 `MysqlServer` 的注释：本项目在「低频」定位下宁可要简单）。
 * `com.sun.net.httpserver` 从 JDK 6 起就在 `java.base` 里，零依赖。
 *
 * ## 两条纪律
 *
 * 1. **抓取不产生对象存储请求**（原则 P4）：`/metrics` 只渲染内存里的注册表，
 *    `/healthz` 只读内存状态，两者都不碰 `ObjectStore`。否则监控会成为
 *    自己监控的系统的负载来源——每一次探活都在给对象存储计费。
 * 2. **不阻塞**：`/metrics` 的渲染是纯内存操作（毫秒级），`/healthz` 不做 IO。
 *    任何一个 handler 都不允许 `runBlocking` 到别处去。
 *
 * 默认**关闭**（`--metrics-port` 不传就不启动）：一个数据库不该在运维没要求时
 * 多开一个监听端口。默认只绑回环地址——指标里有库名表名这类信息，
 * 不该默认暴露在公网上。
 */
class MetricsHttpServer(
    private val host: String = "127.0.0.1",
    private val port: Int = 0,
    /** 健康检查。**必须是非阻塞、无 IO** 的（见类注释）。 */
    private val health: () -> HealthReport = { HealthReport(true, "status=ok\n") },
) : AutoCloseable {

    private var server: HttpServer? = null

    /** 实际绑定的端口；未启动时为 -1。传 `port=0` 时由系统分配（测试用）。 */
    var boundPort: Int = -1
        private set

    fun start() {
        if (server != null) return
        // 同样的翻译：观测端口被占用时也要说清"是哪个端口、怎么查、怎么改"
        val s = try {
            HttpServer.create(InetSocketAddress(host, port), 0)
        } catch (e: java.net.BindException) {
            throw IllegalStateException(
                "观测端点端口 $port 已被占用，无法启动。\n" +
                    "  · 查是谁占用：ss -ltnp | grep :$port\n" +
                    "  · 换一个端口：--metrics-port <n>（或 --no-metrics 关掉观测端点）",
                e,
            )
        }
        // 虚拟线程：抓取是极短的任务，用平台线程池为「偶尔抓一次」常驻一组线程不划算
        s.executor = Executors.newVirtualThreadPerTaskExecutor()
        s.createContext("/metrics") { ex -> handle(ex, contentType = PROMETHEUS_CONTENT_TYPE) { renderMetrics() } }
        s.createContext("/healthz") { ex ->
            val r = try {
                health()
            } catch (t: Throwable) {
                // 健康检查自己抛错时必须是「不健康」，而不是 500 空响应——
                // 那会让「探活失败」与「探活没跑到」在监控上长得一样。
                HealthReport(false, "status=fail\nreason=${t::class.simpleName}: ${t.message}\n")
            }
            handle(ex, contentType = TEXT_CONTENT_TYPE, status = if (r.healthy) 200 else 503) { r.body }
        }
        s.start()
        server = s
        boundPort = s.address.port
    }

    fun stop() {
        server?.stop(0)
        server = null
        boundPort = -1
    }

    override fun close() = stop()

    private fun handle(
        ex: HttpExchange,
        contentType: String,
        status: Int = 200,
        body: () -> String,
    ) {
        try {
            val method = ex.requestMethod
            if (method != "GET" && method != "HEAD") {
                ex.responseHeaders.add("Allow", "GET, HEAD")
                respond(ex, 405, TEXT_CONTENT_TYPE, "method not allowed\n", headOnly = false)
                return
            }
            respond(ex, status, contentType, body(), headOnly = method == "HEAD")
        } catch (t: Throwable) {
            // 观测端点绝不能把异常抛回 HttpServer 的连接处理循环里
            runCatching { respond(ex, 500, TEXT_CONTENT_TYPE, "internal error\n", headOnly = false) }
        } finally {
            ex.close()
        }
    }

    private fun respond(ex: HttpExchange, status: Int, contentType: String, body: String, headOnly: Boolean) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        ex.responseHeaders.add("Content-Type", contentType)
        if (headOnly) {
            ex.sendResponseHeaders(status, -1)
            return
        }
        ex.sendResponseHeaders(status, bytes.size.toLong())
        val out: OutputStream = ex.responseBody
        out.write(bytes)
        out.flush()
    }

    companion object {
        /** Prometheus 官方文本格式的 Content-Type（带版本号，抓取器据此选解析器）。 */
        const val PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
        const val TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"

        private val scrapes = Metrics.registry.counter("yb_metrics_scrapes_total")
        private val scrapeDuration = Metrics.registry.histogram(
            "yb_metrics_scrape_duration_seconds",
            doubleArrayOf(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
        )

        init {
            Metrics.registry.help("yb_metrics_scrapes_total", "/metrics 被抓取的次数")
            Metrics.registry.help("yb_metrics_scrape_duration_seconds", "/metrics 的渲染耗时。它变大说明采集器在做昂贵的事")
        }

        /**
         * 渲染当前指标。
         *
         * 先计数、后渲染：这样**本次抓取自己**也出现在输出里，抓取器不需要等下一次
         * 才能看到自己生效（值会因此比真实次数多 1，单调性不受影响）。
         */
        fun renderMetrics(): String {
            scrapes.inc()
            val t0 = System.nanoTime()
            val text = Metrics.registry.render()
            scrapeDuration.observe((System.nanoTime() - t0) / 1e9)
            return text
        }
    }
}
