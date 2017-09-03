package io.youscan.elasticsearch.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public class MultiYPercolateRequestBuilder extends ActionRequestBuilder<MultiYPercolateRequest, MultiYPercolateResponse, MultiYPercolateRequestBuilder> {

    public MultiYPercolateRequestBuilder(ElasticsearchClient client) {
        super(client, MultiYPercolateAction.INSTANCE, new MultiYPercolateRequest());
    }

    public MultiYPercolateRequestBuilder(ElasticsearchClient client, MultiYPercolateAction action) {
        super(client, action, new MultiYPercolateRequest());
    }

    public MultiYPercolateRequestBuilder add(YPercolateRequest percolateRequest) {
        request.add(percolateRequest);
        return this;
    }

    public MultiYPercolateRequestBuilder add(YPercolateRequestBuilder percolateRequestBuilder) {
        request.add(percolateRequestBuilder);
        return this;
    }

    public MultiYPercolateRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }
}
