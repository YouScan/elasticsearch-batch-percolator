package io.youscan.elasticsearch.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class MultiYPercolateAction  extends Action<MultiYPercolateRequest, MultiYPercolateResponse, MultiYPercolateRequestBuilder> {

    public static final MultiYPercolateAction INSTANCE = new MultiYPercolateAction();
    public static final String NAME = "indices:data/read/mypercolate";

    private MultiYPercolateAction() {
        super(NAME);
    }

    @Override
    public MultiYPercolateResponse newResponse() {
        return new MultiYPercolateResponse();
    }

    @Override
    public MultiYPercolateRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new MultiYPercolateRequestBuilder(client, this);
    }
}


