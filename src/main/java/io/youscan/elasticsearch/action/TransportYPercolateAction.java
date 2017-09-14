package io.youscan.elasticsearch.action;

import com.google.common.collect.Iterables;
import io.youscan.elasticsearch.index.YPercolatorService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.percolator.PercolateException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportYPercolateAction extends TransportBroadcastAction<YPercolateRequest, YPercolateResponse, YPercolateShardRequest, YPercolateShardResponse> {

    private final YPercolatorService percolatorService;
    private final TransportGetAction getAction;

    @Inject
    public TransportYPercolateAction(
            Settings settings,
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            YPercolatorService percolatorService,
            TransportGetAction getAction,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
                settings,
                YPercolateAction.NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                YPercolateRequest.class,
                YPercolateShardRequest.class,
                ThreadPool.Names.PERCOLATE
        );

        this.percolatorService = percolatorService;
        this.getAction = getAction;
    }

    @Override
    protected void doExecute(final Task task, final YPercolateRequest request, final ActionListener<YPercolateResponse> listener) {
        request.startTime = System.currentTimeMillis();
        if (request.getRequest() != null) {
            //create a new get request to make sure it has the same headers and context as the original percolate request
            GetRequest getRequest = new GetRequest(request.getRequest(), request);
            getAction.execute(getRequest, new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getResponse) {
                    if (!getResponse.isExists()) {
                        onFailure(new DocumentMissingException(null, request.getRequest().type(), request.getRequest().id()));
                        return;
                    }

                    BytesReference docSource = getResponse.getSourceAsBytesRef();
                    TransportYPercolateAction.super.doExecute(task, new YPercolateRequest(request, docSource), listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            super.doExecute(task, request, listener);
        }
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, YPercolateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, YPercolateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected YPercolateResponse newResponse(YPercolateRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        return percolatorService.reduce(request, shardsResponses);
    }

    @Override
    protected YPercolateShardRequest newShardRequest(int numShards, ShardRouting shard, YPercolateRequest request) {
        return new YPercolateShardRequest(shard.shardId(), numShards, request);
    }

    @Override
    protected YPercolateShardResponse newShardResponse() {
        return new YPercolateShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, YPercolateRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, request.preference());
    }

    @Override
    protected YPercolateShardResponse shardOperation(YPercolateShardRequest request) {
        try {
            ArrayList<Tuple<Integer, YPercolateShardRequest>> requests = new ArrayList<>(1);
            requests.add(Tuple.tuple(0, request));
            Iterable<YPercolatorService.PercolateResult> responses = percolatorService.percolate(requests, request.shardId());

            // There is a single response. Either failed or not.
            YPercolatorService.PercolateResult r = Iterables.getOnlyElement(responses);
            if(r.isError()){
                throw r.getError();
            } else {
                return r.getResponse();
            }
        } catch (Throwable e) {
            logger.debug("{} failed to percolate", e, request.shardId());
            throw new YPercolateException(request.shardId(), "failed to percolate", e);
        }
    }
}
