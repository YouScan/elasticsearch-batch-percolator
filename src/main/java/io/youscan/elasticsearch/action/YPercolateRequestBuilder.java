package io.youscan.elasticsearch.action;


import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.Map;

public class YPercolateRequestBuilder extends BroadcastOperationRequestBuilder<YPercolateRequest, YPercolateResponse, YPercolateRequestBuilder> {

    private PercolateSourceBuilder sourceBuilder;

    public YPercolateRequestBuilder(ElasticsearchClient client, YPercolateAction action) {
        super(client, action, new YPercolateRequest());
    }

    public YPercolateRequestBuilder setDocumentType(String type) {
        request.documentType(type);
        return this;
    }

    public YPercolateRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public YPercolateRequestBuilder setRouting(String... routings) {
        request.routing(Strings.arrayToCommaDelimitedString(routings));
        return this;
    }

    public YPercolateRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    public YPercolateRequestBuilder setGetRequest(GetRequest getRequest) {
        request.getRequest(getRequest);
        return this;
    }

    public YPercolateRequestBuilder setOnlyCount(boolean onlyCount) {
        request.onlyCount(onlyCount);
        return this;
    }

    public YPercolateRequestBuilder setSize(int size) {
        sourceBuilder().setSize(size);
        return this;
    }

    public YPercolateRequestBuilder setSortByScore(boolean sort) {
        sourceBuilder().setSort(sort);
        return this;
    }

    public YPercolateRequestBuilder addSort(SortBuilder sort) {
        sourceBuilder().addSort(sort);
        return this;
    }

    public YPercolateRequestBuilder setScore(boolean score) {
        sourceBuilder().setTrackScores(score);
        return this;
    }

    public YPercolateRequestBuilder setPercolateDoc(PercolateSourceBuilder.DocBuilder docBuilder) {
        sourceBuilder().setDoc(docBuilder);
        return this;
    }

    public YPercolateRequestBuilder setPercolateQuery(QueryBuilder queryBuilder) {
        sourceBuilder().setQueryBuilder(queryBuilder);
        return this;
    }

    public YPercolateRequestBuilder setHighlightBuilder(HighlightBuilder highlightBuilder) {
        sourceBuilder().setHighlightBuilder(highlightBuilder);
        return this;
    }

    public YPercolateRequestBuilder addAggregation(AbstractAggregationBuilder aggregationBuilder) {
        sourceBuilder().addAggregation(aggregationBuilder);
        return this;
    }

    public YPercolateRequestBuilder setSource(PercolateSourceBuilder source) {
        sourceBuilder = source;
        return this;
    }

    public YPercolateRequestBuilder setSource(Map<String, Object> source) {
        request.source(source);
        return this;
    }

    public YPercolateRequestBuilder setSource(Map<String, Object> source, XContentType contentType) {
        request.source(source, contentType);
        return this;
    }

    public YPercolateRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    public YPercolateRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    public YPercolateRequestBuilder setSource(BytesReference source) {
        request.source(source);
        return this;
    }

    public YPercolateRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    public YPercolateRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    private PercolateSourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new PercolateSourceBuilder();
        }
        return sourceBuilder;
    }

    @Override
    public YPercolateRequest request() {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }
        return request;
    }

    @Override
    protected YPercolateRequest beforeExecute(YPercolateRequest request) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }
        return request;
    }
}
