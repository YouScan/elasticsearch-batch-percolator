package io.youscan.elasticsearch.action;

import com.google.common.collect.Iterables;
import io.youscan.elasticsearch.index.YPercolatorService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
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
import java.util.List;
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
        return reduce(request, shardsResponses, percolatorService);
    }

    public static YPercolateResponse reduce(YPercolateRequest request, AtomicReferenceArray shardsResponses, YPercolatorService percolatorService) {
        int successfulShards = 0;
        int failedShards = 0;

        List<YPercolateShardResponse> shardResults = null;
        List<ShardOperationFailedException> shardFailures = null;

        byte percolatorTypeId = 0x00;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                YPercolateShardResponse percolateShardResponse = (YPercolateShardResponse) shardResponse;
                successfulShards++;
                if (!percolateShardResponse.isEmpty()) {
                    if (shardResults == null) {
                        percolatorTypeId = percolateShardResponse.percolatorTypeId();
                        shardResults = new ArrayList<>();
                    }
                    shardResults.add(percolateShardResponse);
                }
            }
        }

        if (shardResults == null) {
            long tookInMillis = Math.max(1, System.currentTimeMillis() - request.startTime);
            YPercolateResponse.Match[] matches = request.onlyCount() ? null : YPercolateResponse.EMPTY;
            return new YPercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, tookInMillis, matches);
        } else {
            YPercolatorService.ReduceResult result = percolatorService.reduce(percolatorTypeId, shardResults, request);
            long tookInMillis =  Math.max(1, System.currentTimeMillis() - request.startTime);
            return new YPercolateResponse(
                    shardsResponses.length(), successfulShards, failedShards, shardFailures,
                    result.matches(), result.count(), tookInMillis, result.reducedAggregations()
            );
        }
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
            logger.trace("{} failed to percolate", e, request.shardId());
            throw new PercolateException(request.shardId(), "failed to percolate", e);
        }
    }
}
