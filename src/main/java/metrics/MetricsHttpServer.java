package metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * 轻量级 HTTP 服务，暴露 /metrics 端点供 Prometheus 采集。
 * <p>
 * 使用 JDK 内置 HttpServer，无需额外依赖。
 */
public class MetricsHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsHttpServer.class);

    private final int port;
    private final PrometheusExporter exporter;
    private HttpServer server;

    public MetricsHttpServer(int port, MetricRegistry registry) {
        this.port = port;
        this.exporter = new PrometheusExporter(registry);
    }

    /**
     * 启动 HTTP 服务。
     */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", new MetricsHandler());
        server.setExecutor(null); // 使用默认 executor
        server.start();
        log.info("Metrics HTTP 服务已启动，端口: {}, 路径: /metrics", port);
    }

    /**
     * 停止 HTTP 服务。
     */
    public void stop() {
        if (server != null) {
            server.stop(0);
            log.info("Metrics HTTP 服务已停止");
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String body = exporter.export();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
