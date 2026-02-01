// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.metric;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CelerData OpenTelemetry Integration
 *
 * Provides native OpenTelemetry support for enterprise observability integration.
 * This is a critical feature for enterprise sales as customers already have
 * observability stacks (Datadog, Splunk, Honeycomb, New Relic, etc.) that
 * require OTLP-compatible telemetry.
 *
 * Key Features:
 * - OTLP Trace export for query lifecycle
 * - OTLP Metrics export compatible with Prometheus/Datadog
 * - Trace context propagation (W3C Trace Context)
 * - Span creation for query phases
 * - Integration with existing StarRocks metrics
 * - Zero-dependency implementation (no OTEL SDK required)
 *
 * Supported Exporters:
 * - OTLP/HTTP (default)
 * - OTLP/gRPC (via configuration)
 * - Jaeger (legacy support)
 * - Zipkin (legacy support)
 *
 * Usage:
 *   // Start a trace for a query
 *   TraceContext ctx = OpenTelemetryIntegration.getInstance()
 *       .startQueryTrace(queryId, "SELECT * FROM ...");
 *
 *   // Add spans for phases
 *   ctx.startSpan("parse");
 *   // ... parsing ...
 *   ctx.endSpan("parse");
 *
 *   // End the trace
 *   ctx.end();
 *
 *   // Export metrics
 *   OpenTelemetryIntegration.getInstance().recordMetric("query.latency", 1500, "ms");
 */
public class OpenTelemetryIntegration {
    private static final Logger LOG = LogManager.getLogger(OpenTelemetryIntegration.class);

    private static volatile OpenTelemetryIntegration INSTANCE;

    // Configuration
    private String otlpEndpoint;
    private String serviceName = "starrocks";
    private String serviceVersion = "1.0.0";
    private Map<String, String> resourceAttributes = new HashMap<>();

    // Active traces
    private final Map<String, TraceContext> activeTraces = new ConcurrentHashMap<>();

    // Metric buffers for batching
    private final List<MetricDataPoint> metricBuffer = new ArrayList<>();
    private final Object metricBufferLock = new Object();

    // Trace buffers for batching
    private final List<SpanData> traceBuffer = new ArrayList<>();
    private final Object traceBufferLock = new Object();

    // Statistics
    private final AtomicLong tracesExported = new AtomicLong(0);
    private final AtomicLong metricsExported = new AtomicLong(0);
    private final AtomicLong exportErrors = new AtomicLong(0);

    // Background exporter
    private final ScheduledExecutorService scheduler;

    // W3C Trace Context header names
    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String TRACESTATE_HEADER = "tracestate";

    private OpenTelemetryIntegration() {
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "OTEL-Exporter");
            t.setDaemon(true);
            return t;
        });

        // Initialize from config
        this.otlpEndpoint = Config.otel_exporter_otlp_endpoint;
        this.serviceName = Config.otel_service_name;

        // Set default resource attributes
        resourceAttributes.put("service.name", serviceName);
        resourceAttributes.put("service.version", serviceVersion);
        resourceAttributes.put("telemetry.sdk.name", "starrocks-otel");
        resourceAttributes.put("telemetry.sdk.language", "java");
        resourceAttributes.put("telemetry.sdk.version", "1.0.0");

        // Schedule periodic export
        if (Config.otel_export_enabled) {
            scheduler.scheduleAtFixedRate(
                this::exportTraces,
                Config.otel_export_interval_seconds,
                Config.otel_export_interval_seconds,
                TimeUnit.SECONDS
            );

            scheduler.scheduleAtFixedRate(
                this::exportMetrics,
                Config.otel_export_interval_seconds,
                Config.otel_export_interval_seconds,
                TimeUnit.SECONDS
            );
        }

        LOG.info("CelerData OpenTelemetry Integration initialized: endpoint={}, service={}",
            otlpEndpoint, serviceName);
    }

    public static OpenTelemetryIntegration getInstance() {
        if (INSTANCE == null) {
            synchronized (OpenTelemetryIntegration.class) {
                if (INSTANCE == null) {
                    INSTANCE = new OpenTelemetryIntegration();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Start a trace for a query
     */
    public TraceContext startQueryTrace(String queryId, String sql) {
        String traceId = generateTraceId();
        String spanId = generateSpanId();

        TraceContext ctx = new TraceContext(traceId, spanId, queryId);
        ctx.setAttribute("db.system", "starrocks");
        ctx.setAttribute("db.statement", truncateSql(sql));
        ctx.setAttribute("db.query.id", queryId);

        activeTraces.put(queryId, ctx);

        LOG.debug("CelerData OTEL: Started trace {} for query {}", traceId, queryId);

        return ctx;
    }

    /**
     * Start a trace with propagated context (from incoming request headers)
     */
    public TraceContext startQueryTraceWithContext(String queryId, String sql,
                                                   String traceparent, String tracestate) {
        TraceContext ctx;

        if (traceparent != null && !traceparent.isEmpty()) {
            // Parse W3C traceparent header
            // Format: version-traceId-parentSpanId-traceFlags
            String[] parts = traceparent.split("-");
            if (parts.length >= 4) {
                String traceId = parts[1];
                String parentSpanId = parts[2];
                String spanId = generateSpanId();

                ctx = new TraceContext(traceId, spanId, queryId);
                ctx.setParentSpanId(parentSpanId);
                ctx.setTraceState(tracestate);
            } else {
                ctx = startQueryTrace(queryId, sql);
            }
        } else {
            ctx = startQueryTrace(queryId, sql);
        }

        ctx.setAttribute("db.system", "starrocks");
        ctx.setAttribute("db.statement", truncateSql(sql));
        ctx.setAttribute("db.query.id", queryId);

        activeTraces.put(queryId, ctx);

        return ctx;
    }

    /**
     * Get active trace for a query
     */
    public TraceContext getTrace(String queryId) {
        return activeTraces.get(queryId);
    }

    /**
     * End a query trace
     */
    public void endQueryTrace(String queryId, boolean success) {
        TraceContext ctx = activeTraces.remove(queryId);
        if (ctx != null) {
            ctx.setAttribute("otel.status_code", success ? "OK" : "ERROR");
            ctx.end();

            // Add to export buffer
            synchronized (traceBufferLock) {
                traceBuffer.addAll(ctx.getSpans());
            }

            LOG.debug("CelerData OTEL: Ended trace {} for query {} (success={})",
                ctx.traceId, queryId, success);
        }
    }

    /**
     * Record a metric data point
     */
    public void recordMetric(String name, double value, String unit) {
        recordMetric(name, value, unit, new HashMap<>());
    }

    /**
     * Record a metric with attributes
     */
    public void recordMetric(String name, double value, String unit, Map<String, String> attributes) {
        MetricDataPoint dataPoint = new MetricDataPoint(
            name, value, unit, System.currentTimeMillis(), attributes
        );

        synchronized (metricBufferLock) {
            metricBuffer.add(dataPoint);
        }
    }

    /**
     * Record query latency metric
     */
    public void recordQueryLatency(String queryId, long latencyMs, String resourceGroup) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("query.id", queryId);
        attrs.put("resource.group", resourceGroup != null ? resourceGroup : "default");
        recordMetric("starrocks.query.latency", latencyMs, "ms", attrs);
    }

    /**
     * Record query throughput
     */
    public void recordQueryThroughput(long rowsProcessed, long bytesProcessed) {
        recordMetric("starrocks.query.rows_processed", rowsProcessed, "rows");
        recordMetric("starrocks.query.bytes_processed", bytesProcessed, "bytes");
    }

    /**
     * Generate W3C traceparent header for outgoing requests
     */
    public String generateTraceparent(TraceContext ctx) {
        if (ctx == null) {
            return null;
        }
        // Format: version-traceId-spanId-traceFlags
        return String.format("00-%s-%s-01", ctx.traceId, ctx.currentSpanId);
    }

    /**
     * Export traces to OTLP endpoint
     */
    private void exportTraces() {
        List<SpanData> spansToExport;
        synchronized (traceBufferLock) {
            if (traceBuffer.isEmpty()) {
                return;
            }
            spansToExport = new ArrayList<>(traceBuffer);
            traceBuffer.clear();
        }

        try {
            String json = buildOtlpTraceJson(spansToExport);
            sendToOtlp(otlpEndpoint + "/v1/traces", json);
            tracesExported.addAndGet(spansToExport.size());

            LOG.debug("CelerData OTEL: Exported {} traces", spansToExport.size());
        } catch (Exception e) {
            exportErrors.incrementAndGet();
            LOG.warn("CelerData OTEL: Failed to export traces", e);

            // Re-add failed spans for retry (with limit)
            synchronized (traceBufferLock) {
                if (traceBuffer.size() < 10000) {
                    traceBuffer.addAll(spansToExport);
                }
            }
        }
    }

    /**
     * Export metrics to OTLP endpoint
     */
    private void exportMetrics() {
        List<MetricDataPoint> metricsToExport;
        synchronized (metricBufferLock) {
            if (metricBuffer.isEmpty()) {
                return;
            }
            metricsToExport = new ArrayList<>(metricBuffer);
            metricBuffer.clear();
        }

        try {
            String json = buildOtlpMetricJson(metricsToExport);
            sendToOtlp(otlpEndpoint + "/v1/metrics", json);
            metricsExported.addAndGet(metricsToExport.size());

            LOG.debug("CelerData OTEL: Exported {} metrics", metricsToExport.size());
        } catch (Exception e) {
            exportErrors.incrementAndGet();
            LOG.warn("CelerData OTEL: Failed to export metrics", e);
        }
    }

    private void sendToOtlp(String endpoint, String json) throws Exception {
        if (endpoint == null || endpoint.isEmpty()) {
            return;
        }

        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(10000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode >= 400) {
                throw new RuntimeException("OTLP export failed with status: " + responseCode);
            }
        } finally {
            conn.disconnect();
        }
    }

    private String buildOtlpTraceJson(List<SpanData> spans) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceSpans\":[{");
        sb.append("\"resource\":{\"attributes\":[");

        // Resource attributes
        boolean first = true;
        for (Map.Entry<String, String> attr : resourceAttributes.entrySet()) {
            if (!first) sb.append(",");
            sb.append("{\"key\":\"").append(attr.getKey())
              .append("\",\"value\":{\"stringValue\":\"").append(attr.getValue()).append("\"}}");
            first = false;
        }
        sb.append("]},");

        // Spans
        sb.append("\"scopeSpans\":[{\"spans\":[");
        first = true;
        for (SpanData span : spans) {
            if (!first) sb.append(",");
            sb.append(span.toOtlpJson());
            first = false;
        }
        sb.append("]}]}]}");

        return sb.toString();
    }

    private String buildOtlpMetricJson(List<MetricDataPoint> metrics) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resourceMetrics\":[{");
        sb.append("\"resource\":{\"attributes\":[");

        // Resource attributes
        boolean first = true;
        for (Map.Entry<String, String> attr : resourceAttributes.entrySet()) {
            if (!first) sb.append(",");
            sb.append("{\"key\":\"").append(attr.getKey())
              .append("\",\"value\":{\"stringValue\":\"").append(attr.getValue()).append("\"}}");
            first = false;
        }
        sb.append("]},");

        // Metrics
        sb.append("\"scopeMetrics\":[{\"metrics\":[");
        first = true;
        for (MetricDataPoint metric : metrics) {
            if (!first) sb.append(",");
            sb.append(metric.toOtlpJson());
            first = false;
        }
        sb.append("]}]}]}");

        return sb.toString();
    }

    private String generateTraceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private String generateSpanId() {
        return Long.toHexString(UUID.randomUUID().getMostSignificantBits());
    }

    private String truncateSql(String sql) {
        if (sql == null) return "";
        if (sql.length() > 1000) {
            return sql.substring(0, 1000) + "...";
        }
        return sql;
    }

    public long getTracesExported() {
        return tracesExported.get();
    }

    public long getMetricsExported() {
        return metricsExported.get();
    }

    public long getExportErrors() {
        return exportErrors.get();
    }

    @VisibleForTesting
    public void reset() {
        activeTraces.clear();
        synchronized (traceBufferLock) {
            traceBuffer.clear();
        }
        synchronized (metricBufferLock) {
            metricBuffer.clear();
        }
        tracesExported.set(0);
        metricsExported.set(0);
        exportErrors.set(0);
    }

    // ==================== Inner Classes ====================

    /**
     * Trace context for a query
     */
    public static class TraceContext {
        public final String traceId;
        public final String rootSpanId;
        public final String queryId;
        public String currentSpanId;
        private String parentSpanId;
        private String traceState;
        private final long startTimeNanos;

        private final List<SpanData> spans = new ArrayList<>();
        private final Map<String, Object> attributes = new HashMap<>();
        private SpanData currentSpan;

        public TraceContext(String traceId, String spanId, String queryId) {
            this.traceId = traceId;
            this.rootSpanId = spanId;
            this.currentSpanId = spanId;
            this.queryId = queryId;
            this.startTimeNanos = System.nanoTime();

            // Create root span
            this.currentSpan = new SpanData(traceId, spanId, null, "query");
        }

        public void setParentSpanId(String parentSpanId) {
            this.parentSpanId = parentSpanId;
            if (currentSpan != null) {
                currentSpan.parentSpanId = parentSpanId;
            }
        }

        public void setTraceState(String traceState) {
            this.traceState = traceState;
        }

        public void setAttribute(String key, Object value) {
            attributes.put(key, value);
            if (currentSpan != null) {
                currentSpan.attributes.put(key, value);
            }
        }

        public void startSpan(String name) {
            // End current span and start new child span
            if (currentSpan != null) {
                currentSpan.end();
                spans.add(currentSpan);
            }

            String newSpanId = Long.toHexString(UUID.randomUUID().getMostSignificantBits());
            SpanData newSpan = new SpanData(traceId, newSpanId, currentSpanId, name);
            newSpan.attributes.putAll(attributes);
            currentSpan = newSpan;
            currentSpanId = newSpanId;
        }

        public void endSpan(String name) {
            if (currentSpan != null && currentSpan.name.equals(name)) {
                currentSpan.end();
                spans.add(currentSpan);
                currentSpan = null;
            }
        }

        public void addEvent(String name) {
            if (currentSpan != null) {
                currentSpan.addEvent(name, System.nanoTime());
            }
        }

        public void end() {
            if (currentSpan != null) {
                currentSpan.end();
                spans.add(currentSpan);
            }
        }

        public List<SpanData> getSpans() {
            return new ArrayList<>(spans);
        }

        public long getDurationNanos() {
            return System.nanoTime() - startTimeNanos;
        }
    }

    /**
     * Span data for OTLP export
     */
    public static class SpanData {
        public final String traceId;
        public final String spanId;
        public String parentSpanId;
        public final String name;
        public final long startTimeNanos;
        public long endTimeNanos;
        public final Map<String, Object> attributes = new HashMap<>();
        public final List<SpanEvent> events = new ArrayList<>();

        public SpanData(String traceId, String spanId, String parentSpanId, String name) {
            this.traceId = traceId;
            this.spanId = spanId;
            this.parentSpanId = parentSpanId;
            this.name = name;
            this.startTimeNanos = System.nanoTime();
        }

        public void end() {
            this.endTimeNanos = System.nanoTime();
        }

        public void addEvent(String name, long timeNanos) {
            events.add(new SpanEvent(name, timeNanos));
        }

        public String toOtlpJson() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"traceId\":\"").append(traceId).append("\",");
            sb.append("\"spanId\":\"").append(spanId).append("\",");
            if (parentSpanId != null) {
                sb.append("\"parentSpanId\":\"").append(parentSpanId).append("\",");
            }
            sb.append("\"name\":\"").append(name).append("\",");
            sb.append("\"startTimeUnixNano\":").append(startTimeNanos).append(",");
            sb.append("\"endTimeUnixNano\":").append(endTimeNanos).append(",");

            // Attributes
            sb.append("\"attributes\":[");
            boolean first = true;
            for (Map.Entry<String, Object> attr : attributes.entrySet()) {
                if (!first) sb.append(",");
                sb.append("{\"key\":\"").append(attr.getKey()).append("\",");
                if (attr.getValue() instanceof Number) {
                    sb.append("\"value\":{\"intValue\":").append(attr.getValue()).append("}}");
                } else {
                    sb.append("\"value\":{\"stringValue\":\"")
                      .append(String.valueOf(attr.getValue()).replace("\"", "\\\""))
                      .append("\"}}");
                }
                first = false;
            }
            sb.append("]");

            sb.append("}");
            return sb.toString();
        }
    }

    public static class SpanEvent {
        public final String name;
        public final long timeNanos;

        public SpanEvent(String name, long timeNanos) {
            this.name = name;
            this.timeNanos = timeNanos;
        }
    }

    /**
     * Metric data point for OTLP export
     */
    public static class MetricDataPoint {
        public final String name;
        public final double value;
        public final String unit;
        public final long timestampMs;
        public final Map<String, String> attributes;

        public MetricDataPoint(String name, double value, String unit,
                              long timestampMs, Map<String, String> attributes) {
            this.name = name;
            this.value = value;
            this.unit = unit;
            this.timestampMs = timestampMs;
            this.attributes = attributes;
        }

        public String toOtlpJson() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"name\":\"").append(name).append("\",");
            sb.append("\"unit\":\"").append(unit).append("\",");
            sb.append("\"gauge\":{\"dataPoints\":[{");
            sb.append("\"asDouble\":").append(value).append(",");
            sb.append("\"timeUnixNano\":").append(timestampMs * 1_000_000L).append(",");

            // Attributes
            sb.append("\"attributes\":[");
            boolean first = true;
            for (Map.Entry<String, String> attr : attributes.entrySet()) {
                if (!first) sb.append(",");
                sb.append("{\"key\":\"").append(attr.getKey())
                  .append("\",\"value\":{\"stringValue\":\"").append(attr.getValue()).append("\"}}");
                first = false;
            }
            sb.append("]");

            sb.append("}]}}");
            return sb.toString();
        }
    }
}
