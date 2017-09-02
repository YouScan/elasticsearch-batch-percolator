package io.youscan.elasticsearch.rest;

import io.youscan.elasticsearch.action.YPercolateAction;
import io.youscan.elasticsearch.action.YPercolateRequest;
import io.youscan.elasticsearch.action.YPercolateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestYPercolateAction extends BaseRestHandler {

    @Inject
    public RestYPercolateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_ypercolate", this);
        controller.registerHandler(POST, "/{index}/_ypercolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_ypercolate", this);

        controller.registerHandler(GET, "/_ypercolate", this);
        controller.registerHandler(GET, "/{index}/_ypercolate", this);
        controller.registerHandler(GET, "/{index}/{type}/_ypercolate", this);
    }

    @Override
    public void handleRequest(final RestRequest restRequest, final RestChannel restChannel, final Client client) throws Exception {
        YPercolateRequest percolateRequest = new YPercolateRequest();
        percolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, percolateRequest.indicesOptions()));
        percolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        percolateRequest.documentType(restRequest.param("type"));
        // percolateRequest.documentType(percolateRequest.documentType()); WTF?
        percolateRequest.source(RestActions.getRestContent(restRequest));

        client.execute(YPercolateAction.INSTANCE,
                percolateRequest,
                new RestToXContentListener<YPercolateResponse>(restChannel)
        );
    }
}