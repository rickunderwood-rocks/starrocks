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

package com.starrocks.http.rest;

/**
 * CelerData Async Query REST API for AI Agents
 *
 * This API implements a submit/poll/cancel pattern optimized for AI agents and
 * automated systems that need non-blocking query execution.
 *
 * Endpoints:
 *   POST /api/v1/async_query/submit     - Submit a query, returns query_id immediately
 *   GET  /api/v1/async_query/{id}/status - Check query execution status
 *   GET  /api/v1/async_query/{id}/result - Retrieve results (when status is COMPLETED)
 *   POST /api/v1/async_query/{id}/cancel - Cancel a running query
 *
 * Example usage:
 *   1. Submit: curl -X POST '/api/v1/async_query/submit' -d '{"query": "SELECT * FROM t"}'
 *      Response: {"query_id": "abc123", "status": "SUBMITTED"}
 *
 *   2. Poll: curl '/api/v1/async_query/abc123/status'
 *      Response: {"query_id": "abc123", "status": "RUNNING", "progress": 45}
 *
 *   3. Get Result: curl '/api/v1/async_query/abc123/result'
 *      Response: {"query_id": "abc123", "status": "COMPLETED", "data": [...], "meta": [...]}
 *
 *   4. Cancel: curl -X POST '/api/v1/async_query/abc123/cancel'
 *      Response: {"query_id": "abc123", "status": "CANCELLED"}
 */

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

/**
 * Async Query Action for AI Agent integration.
 * Provides non-blocking query execution with submit/poll/cancel semantics.
 */
public class AsyncQueryAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(AsyncQueryAction.class);

    private static final String QUERY_ID_KEY = "query_id";

    // Query status enum
    public enum QueryStatus {
        SUBMITTED,   // Query accepted, not yet started
        RUNNING,     // Query is executing
        COMPLETED,   // Query finished successfully
        FAILED,      // Query failed with error
        CANCELLED,   // Query was cancelled
        TIMEOUT      // Query exceeded time limit
    }

    // In-memory store for async queries (TODO: Replace with distributed store for HA)
    private static final ConcurrentHashMap<String, AsyncQueryState> queryStore = new ConcurrentHashMap<>();

    // Maximum queries to keep in memory
    private static final int MAX_CACHED_QUERIES = 10000;

    // Query result TTL in milliseconds (1 hour)
    private static final long RESULT_TTL_MS = 3600000;

    public AsyncQueryAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        AsyncQueryAction action = new AsyncQueryAction(controller);

        // Submit a new async query
        controller.registerHandler(HttpMethod.POST,
                "/api/v1/async_query/submit",
                action);

        // Get query status
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/async_query/{" + QUERY_ID_KEY + "}/status",
                action);

        // Get query results
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/async_query/{" + QUERY_ID_KEY + "}/result",
                action);

        // Cancel a query
        controller.registerHandler(HttpMethod.POST,
                "/api/v1/async_query/{" + QUERY_ID_KEY + "}/cancel",
                action);
    }

    @Override
    public boolean supportAsyncHandler() {
        return true;
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        response.setContentType("application/json; charset=utf-8");

        String uri = request.getRequest().uri();
        HttpMethod method = request.getRequest().method();

        try {
            if (method == HttpMethod.POST && uri.contains("/submit")) {
                handleSubmit(request, response);
            } else if (method == HttpMethod.GET && uri.contains("/status")) {
                handleStatus(request, response);
            } else if (method == HttpMethod.GET && uri.contains("/result")) {
                handleResult(request, response);
            } else if (method == HttpMethod.POST && uri.contains("/cancel")) {
                handleCancel(request, response);
            } else {
                throw new StarRocksHttpException(BAD_REQUEST, "Unknown async query endpoint");
            }
        } catch (StarRocksHttpException e) {
            LOG.warn("Async query error: {}", e.getMessage());
            AsyncQueryResponse errorResponse = new AsyncQueryResponse();
            errorResponse.status = QueryStatus.FAILED.name();
            errorResponse.error = e.getMessage();
            response.getContent().append(new Gson().toJson(errorResponse));
            writeResponse(request, response, HttpResponseStatus.valueOf(e.getCode().code()));
        }
    }

    /**
     * Handle POST /api/v1/async_query/submit
     * Accepts a query and returns a query_id for polling.
     */
    private void handleSubmit(BaseRequest request, BaseResponse response) throws StarRocksHttpException {
        // Parse request body
        AsyncQueryRequest queryRequest = parseRequestBody(request.getContent());

        // Generate unique query ID
        String queryId = UUIDUtil.genUUID().toString();

        // Create query state
        AsyncQueryState state = new AsyncQueryState();
        state.queryId = queryId;
        state.query = queryRequest.query;
        state.status = QueryStatus.SUBMITTED;
        state.submitTime = System.currentTimeMillis();
        state.catalog = queryRequest.catalog;
        state.database = queryRequest.database;

        // Store the query state
        storeQueryState(queryId, state);

        // TODO: Submit query to execution engine asynchronously
        // For now, this is a scaffold - actual execution will be added
        // ExecutorService.submit(() -> executeQueryAsync(state));

        LOG.info("Async query submitted: id={}, query={}", queryId, queryRequest.query);

        // Return response
        AsyncQueryResponse resp = new AsyncQueryResponse();
        resp.queryId = queryId;
        resp.status = QueryStatus.SUBMITTED.name();
        resp.message = "Query submitted successfully. Poll /status endpoint for progress.";

        response.getContent().append(new Gson().toJson(resp));
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    /**
     * Handle GET /api/v1/async_query/{query_id}/status
     * Returns the current status of a query.
     */
    private void handleStatus(BaseRequest request, BaseResponse response) throws StarRocksHttpException {
        String queryId = request.getSingleParameter(QUERY_ID_KEY);

        AsyncQueryState state = getQueryState(queryId);
        if (state == null) {
            throw new StarRocksHttpException(NOT_FOUND, "Query not found: " + queryId);
        }

        AsyncQueryResponse resp = new AsyncQueryResponse();
        resp.queryId = queryId;
        resp.status = state.status.name();
        resp.progress = state.progress;
        resp.startTime = state.startTime;
        resp.endTime = state.endTime;

        if (state.status == QueryStatus.FAILED) {
            resp.error = state.errorMessage;
        }

        response.getContent().append(new Gson().toJson(resp));
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    /**
     * Handle GET /api/v1/async_query/{query_id}/result
     * Returns query results if completed.
     */
    private void handleResult(BaseRequest request, BaseResponse response) throws StarRocksHttpException {
        String queryId = request.getSingleParameter(QUERY_ID_KEY);

        AsyncQueryState state = getQueryState(queryId);
        if (state == null) {
            throw new StarRocksHttpException(NOT_FOUND, "Query not found: " + queryId);
        }

        AsyncQueryResponse resp = new AsyncQueryResponse();
        resp.queryId = queryId;
        resp.status = state.status.name();

        if (state.status == QueryStatus.COMPLETED) {
            resp.data = state.resultData;
            resp.meta = state.resultMeta;
            resp.rowCount = state.rowCount;
        } else if (state.status == QueryStatus.FAILED) {
            resp.error = state.errorMessage;
        } else if (state.status == QueryStatus.RUNNING || state.status == QueryStatus.SUBMITTED) {
            resp.message = "Query still executing. Poll /status endpoint for progress.";
            resp.progress = state.progress;
        }

        response.getContent().append(new Gson().toJson(resp));
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    /**
     * Handle POST /api/v1/async_query/{query_id}/cancel
     * Cancels a running query.
     */
    private void handleCancel(BaseRequest request, BaseResponse response) throws StarRocksHttpException {
        String queryId = request.getSingleParameter(QUERY_ID_KEY);

        AsyncQueryState state = getQueryState(queryId);
        if (state == null) {
            throw new StarRocksHttpException(NOT_FOUND, "Query not found: " + queryId);
        }

        if (state.status == QueryStatus.RUNNING || state.status == QueryStatus.SUBMITTED) {
            state.status = QueryStatus.CANCELLED;
            state.endTime = System.currentTimeMillis();

            // TODO: Actually cancel the running query execution
            // KillStmt killStmt = new KillStmt(state.connectionId, true);

            LOG.info("Async query cancelled: id={}", queryId);
        }

        AsyncQueryResponse resp = new AsyncQueryResponse();
        resp.queryId = queryId;
        resp.status = state.status.name();
        resp.message = "Query cancelled";

        response.getContent().append(new Gson().toJson(resp));
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    private AsyncQueryRequest parseRequestBody(String content) throws StarRocksHttpException {
        try {
            Type type = new TypeToken<AsyncQueryRequest>() {}.getType();
            AsyncQueryRequest req = new Gson().fromJson(content, type);

            if (req == null || Strings.isNullOrEmpty(req.query)) {
                throw new StarRocksHttpException(BAD_REQUEST, "Query cannot be empty");
            }

            return req;
        } catch (JsonSyntaxException e) {
            throw new StarRocksHttpException(BAD_REQUEST, "Invalid JSON: " + e.getMessage());
        }
    }

    private void storeQueryState(String queryId, AsyncQueryState state) {
        // Simple eviction if we have too many queries
        if (queryStore.size() >= MAX_CACHED_QUERIES) {
            evictOldQueries();
        }
        queryStore.put(queryId, state);
    }

    private AsyncQueryState getQueryState(String queryId) {
        return queryStore.get(queryId);
    }

    private void evictOldQueries() {
        long now = System.currentTimeMillis();
        queryStore.entrySet().removeIf(entry -> {
            AsyncQueryState state = entry.getValue();
            // Remove completed/failed/cancelled queries older than TTL
            if (state.status != QueryStatus.RUNNING && state.status != QueryStatus.SUBMITTED) {
                return (now - state.endTime) > RESULT_TTL_MS;
            }
            return false;
        });
    }

    // Request/Response DTOs

    static class AsyncQueryRequest {
        public String query;
        public String catalog;
        public String database;
        public Map<String, String> sessionVariables;
        public Long timeoutMs;
    }

    static class AsyncQueryResponse {
        public String queryId;
        public String status;
        public String message;
        public String error;
        public Integer progress;
        public Long startTime;
        public Long endTime;
        public Object data;
        public Object meta;
        public Long rowCount;
    }

    static class AsyncQueryState {
        public String queryId;
        public String query;
        public QueryStatus status;
        public String catalog;
        public String database;
        public long submitTime;
        public long startTime;
        public long endTime;
        public int progress;
        public String errorMessage;
        public Object resultData;
        public Object resultMeta;
        public long rowCount;
        public long connectionId;
    }
}
