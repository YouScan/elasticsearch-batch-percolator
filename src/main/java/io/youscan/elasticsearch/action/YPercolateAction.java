package io.youscan.elasticsearch.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class YPercolateAction extends Action<YPercolateRequest, YPercolateResponse, YPercolateRequestBuilder> {

    public static final YPercolateAction INSTANCE = new YPercolateAction();
    public static final String NAME = "indices:data/read/ypercolate";

    private YPercolateAction() {
        super(NAME);
    }

    @Override
    public YPercolateResponse newResponse() {
        return new YPercolateResponse();
    }

    @Override
    public YPercolateRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new YPercolateRequestBuilder(client, this);
    }
}