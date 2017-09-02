package io.youscan.elasticsearch.rest;

import io.youscan.elasticsearch.action.MultiYPercolateAction;
import io.youscan.elasticsearch.action.MultiYPercolateRequest;
import io.youscan.elasticsearch.action.MultiYPercolateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiYPercolateAction extends BaseRestHandler {

    @Inject
    public RestMultiYPercolateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_mypercolate", this);
        controller.registerHandler(POST, "/{index}/_mypercolate", this);
        controller.registerHandler(POST, "/{index}/{type}/_mypercolate", this);

        controller.registerHandler(GET, "/_mypercolate", this);
        controller.registerHandler(GET, "/{index}/_mypercolate", this);
        controller.registerHandler(GET, "/{index}/{type}/_mypercolate", this);
    }

    @Override
    public void handleRequest(final RestRequest restRequest, final RestChannel restChannel, final Client client) throws Exception {

        MultiYPercolateRequest percolateRequest = new MultiYPercolateRequest();
        percolateRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, percolateRequest.indicesOptions()));
        percolateRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        percolateRequest.documentType(restRequest.param("type"));

        // percolateRequest.documentType(percolateRequest.documentType()); WTF?

        // TODO initialize all missing fields
        // percolateRequest.source(RestActions.getRestContent(restRequest));

        client.execute(MultiYPercolateAction.INSTANCE,
                percolateRequest,
                new RestToXContentListener<MultiYPercolateResponse>(restChannel)
        );
    }
}
