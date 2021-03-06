package io.youscan.elasticsearch.action;

import com.carrotsearch.hppc.IntArrayList;
import io.youscan.elasticsearch.index.YPercolatorService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportMultiYPercolateAction  extends HandledTransportAction<MultiYPercolateRequest, MultiYPercolateResponse> {

    private final ClusterService clusterService;
    private final YPercolatorService percolatorService;

    private final TransportMultiGetAction multiGetAction;
    private final TransportShardMultiYPercolateAction shardMultiPercolateAction;

    @Inject
    public TransportMultiYPercolateAction(
            Settings settings,
            ThreadPool threadPool,
            TransportShardMultiYPercolateAction shardMultiPercolateAction,
            ClusterService clusterService,
            TransportService transportService,
            YPercolatorService percolatorService,
            TransportMultiGetAction multiGetAction,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
    ) {

        super(settings, MultiYPercolateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, MultiYPercolateRequest.class);

        this.shardMultiPercolateAction = shardMultiPercolateAction;
        this.clusterService = clusterService;
        this.percolatorService = percolatorService;
        this.multiGetAction = multiGetAction;
    }

    @Override
    protected void doExecute(final MultiYPercolateRequest request, final ActionListener<MultiYPercolateResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final List<Object> percolateRequests = new ArrayList<>(request.requests().size());
        // Can have a mixture of percolate requests. (normal percolate requests & percolate existing doc),
        // so we need to keep track for what percolate request we had a get request
        final IntArrayList getRequestSlots = new IntArrayList();
        List<GetRequest> existingDocsRequests = new ArrayList<>();
        for (int slot = 0;  slot < request.requests().size(); slot++) {
            YPercolateRequest percolateRequest = request.requests().get(slot);
            percolateRequest.startTime = System.currentTimeMillis();
            percolateRequests.add(percolateRequest);
            if (percolateRequest.getRequest() != null) {
                existingDocsRequests.add(percolateRequest.getRequest());
                getRequestSlots.add(slot);
            }
        }

        if (!existingDocsRequests.isEmpty()) {
            final MultiGetRequest multiGetRequest = new MultiGetRequest(request);
            for (GetRequest getRequest : existingDocsRequests) {
                multiGetRequest.add(
                        new MultiGetRequest.Item(getRequest.index(), getRequest.type(), getRequest.id())
                                .routing(getRequest.routing())
                );
            }

            multiGetAction.execute(multiGetRequest, new ActionListener<MultiGetResponse>() {

                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    for (int i = 0; i < multiGetItemResponses.getResponses().length; i++) {
                        MultiGetItemResponse itemResponse = multiGetItemResponses.getResponses()[i];
                        int slot = getRequestSlots.get(i);
                        if (!itemResponse.isFailed()) {
                            GetResponse getResponse = itemResponse.getResponse();
                            if (getResponse.isExists()) {
                                YPercolateRequest originalRequest = (YPercolateRequest) percolateRequests.get(slot);
                                percolateRequests.set(slot, new YPercolateRequest(originalRequest, getResponse.getSourceAsBytesRef()));
                            } else {
                                logger.trace("mypercolate existing doc, item[{}] doesn't exist", slot);
                                percolateRequests.set(slot, new DocumentMissingException(null, getResponse.getType(), getResponse.getId()));
                            }
                        } else {
                            logger.trace("mypercolate existing doc, item[{}] failure {}", slot, itemResponse.getFailure());
                            percolateRequests.set(slot, itemResponse.getFailure());
                        }
                    }
                    new ASyncAction(request, percolateRequests, listener, clusterState).run();
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            new ASyncAction(request, percolateRequests, listener, clusterState).run();
        }
    }

    private final class ASyncAction {

        final ActionListener<MultiYPercolateResponse> finalListener;
        final Map<ShardId, TransportShardMultiYPercolateAction.Request> requestsByShard;
        final MultiYPercolateRequest multiPercolateRequest;
        final List<Object> percolateRequests;

        final Map<ShardId, IntArrayList> shardToSlots;
        final AtomicInteger expectedOperations;
        final AtomicArray<Object> reducedResponses;
        final AtomicReferenceArray<AtomicInteger> expectedOperationsPerItem;
        final AtomicReferenceArray<AtomicReferenceArray> responsesByItemAndShard;

        ASyncAction(MultiYPercolateRequest multiPercolateRequest, List<Object> percolateRequests, ActionListener<MultiYPercolateResponse> finalListener, ClusterState clusterState) {
            this.finalListener = finalListener;
            this.multiPercolateRequest = multiPercolateRequest;
            this.percolateRequests = percolateRequests;
            responsesByItemAndShard = new AtomicReferenceArray<>(percolateRequests.size());
            expectedOperationsPerItem = new AtomicReferenceArray<>(percolateRequests.size());
            reducedResponses = new AtomicArray<>(percolateRequests.size());

            // Resolving concrete indices and routing and grouping the requests by shard
            requestsByShard = new HashMap<>();
            // Keep track what slots belong to what shard, in case a request to a shard fails on all copies
            shardToSlots = new HashMap<>();
            int expectedResults = 0;
            for (int slot = 0;  slot < percolateRequests.size(); slot++) {
                Object element = percolateRequests.get(slot);
                assert element != null;
                if (element instanceof YPercolateRequest) {
                    YPercolateRequest percolateRequest = (YPercolateRequest) element;
                    String[] concreteIndices;
                    try {
                        concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, percolateRequest);
                    } catch (IndexNotFoundException e) {
                        reducedResponses.set(slot, e);
                        responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                        expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                        continue;
                    }
                    Map<String, Set<String>> routing = indexNameExpressionResolver.resolveSearchRouting(clusterState, percolateRequest.routing(), percolateRequest.indices());
                    // TODO: I only need shardIds, ShardIterator(ShardRouting) is only needed in TransportShardMultiYPercolateAction
                    GroupShardsIterator shards = clusterService.operationRouting().searchShards(
                            clusterState, concreteIndices, routing, percolateRequest.preference()
                    );
                    if (shards.size() == 0) {
                        reducedResponses.set(slot, new UnavailableShardsException(null, "No shards available"));
                        responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                        expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                        continue;
                    }

                    // The shard id is used as index in the atomic ref array, so we need to find out how many shards there are regardless of routing:
                    int numShards = clusterService.operationRouting().searchShardsCount(clusterState, concreteIndices, null);
                    responsesByItemAndShard.set(slot, new AtomicReferenceArray(numShards));
                    expectedOperationsPerItem.set(slot, new AtomicInteger(shards.size()));
                    for (ShardIterator shard : shards) {
                        ShardId shardId = shard.shardId();
                        TransportShardMultiYPercolateAction.Request requests = requestsByShard.get(shardId);
                        if (requests == null) {
                            requestsByShard.put(shardId, requests = new TransportShardMultiYPercolateAction.Request(multiPercolateRequest, shardId.getIndex(), shardId.getId(), percolateRequest.preference()));
                        }
                        logger.trace("Adding shard[{}] percolate request for item[{}]", shardId, slot);
                        requests.add(new TransportShardMultiYPercolateAction.Request.Item(slot, new YPercolateShardRequest(shardId, percolateRequest)));

                        IntArrayList items = shardToSlots.get(shardId);
                        if (items == null) {
                            shardToSlots.put(shardId, items = new IntArrayList());
                        }
                        items.add(slot);
                    }
                    expectedResults++;
                } else if (element instanceof Throwable || element instanceof MultiGetResponse.Failure) {
                    logger.trace("item[{}] won't be executed, reason: {}", slot, element);
                    reducedResponses.set(slot, element);
                    responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                    expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                }
            }
            expectedOperations = new AtomicInteger(expectedResults);
        }

        void run() {
            if (expectedOperations.get() == 0) {
                finish();
                return;
            }

            logger.trace("mypercolate executing for shards {}", requestsByShard.keySet());
            for (Map.Entry<ShardId, TransportShardMultiYPercolateAction.Request> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                TransportShardMultiYPercolateAction.Request shardRequest = entry.getValue();
                shardMultiPercolateAction.execute(shardRequest, new ActionListener<TransportShardMultiYPercolateAction.Response>() {

                    @Override
                    public void onResponse(TransportShardMultiYPercolateAction.Response response) {
                        onShardResponse(shardId, response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        onShardFailure(shardId, e);
                    }

                });
            }
        }

        @SuppressWarnings("unchecked")
        void onShardResponse(ShardId shardId, TransportShardMultiYPercolateAction.Response response) {
            logger.trace("{} Percolate shard response", shardId);
            try {
                for (TransportShardMultiYPercolateAction.Response.Item item : response.items()) {
                    AtomicReferenceArray shardResults = responsesByItemAndShard.get(item.slot());
                    if (shardResults == null) {
                        assert false : "shardResults can't be null";
                        continue;
                    }

                    if (item.failed()) {
                        shardResults.set(shardId.id(), new BroadcastShardOperationFailedException(shardId, item.error()));
                    } else {
                        shardResults.set(shardId.id(), item.response());
                    }

                    assert expectedOperationsPerItem.get(item.slot()).get() >= 1 : "slot[" + item.slot() + "] can't be lower than one";
                    if (expectedOperationsPerItem.get(item.slot()).decrementAndGet() == 0) {
                        // Failure won't bubble up, since we fail the whole request now via the catch clause below,
                        // so expectedOperationsPerItem will not be decremented twice.
                        reduce(item.slot());
                    }
                }
            } catch (Throwable e) {
                logger.error("{} Percolate original reduce error", e, shardId);
                finalListener.onFailure(e);
            }
        }

        @SuppressWarnings("unchecked")
        void onShardFailure(ShardId shardId, Throwable e) {
            logger.debug("{} Shard multi percolate failure", e, shardId);
            try {
                IntArrayList slots = shardToSlots.get(shardId);
                for (int i = 0; i < slots.size(); i++) {
                    int slot = slots.get(i);
                    AtomicReferenceArray shardResults = responsesByItemAndShard.get(slot);
                    if (shardResults == null) {
                        continue;
                    }

                    shardResults.set(shardId.id(), new BroadcastShardOperationFailedException(shardId, e));
                    assert expectedOperationsPerItem.get(slot).get() >= 1 : "slot[" + slot + "] can't be lower than one. Caused by: " + e.getMessage();
                    if (expectedOperationsPerItem.get(slot).decrementAndGet() == 0) {
                        reduce(slot);
                    }
                }
            } catch (Throwable t) {
                logger.error("{} Percolate original reduce error, original error {}", t, shardId, e);
                finalListener.onFailure(t);
            }
        }

        void reduce(int slot) {
            AtomicReferenceArray shardResponses = responsesByItemAndShard.get(slot);
            YPercolateResponse reducedResponse = percolatorService.reduce((YPercolateRequest) percolateRequests.get(slot), shardResponses);
            reducedResponses.set(slot, reducedResponse);
            assert expectedOperations.get() >= 1 : "slot[" + slot + "] expected options should be >= 1 but is " + expectedOperations.get();
            if (expectedOperations.decrementAndGet() == 0) {
                finish();
            }
        }

        void finish() {
            MultiYPercolateResponse.Item[] finalResponse = new MultiYPercolateResponse.Item[reducedResponses.length()];
            for (int slot = 0; slot < reducedResponses.length(); slot++) {
                Object element = reducedResponses.get(slot);
                assert element != null : "Element[" + slot + "] shouldn't be null";
                if (element instanceof YPercolateResponse) {
                    finalResponse[slot] = new MultiYPercolateResponse.Item((YPercolateResponse) element);
                } else if (element instanceof Throwable) {
                    finalResponse[slot] = new MultiYPercolateResponse.Item((Throwable)element);
                } else if (element instanceof MultiGetResponse.Failure) {
                    finalResponse[slot] = new MultiYPercolateResponse.Item(((MultiGetResponse.Failure)element).getFailure());
                }
            }
            finalListener.onResponse(new MultiYPercolateResponse(finalResponse));
        }

    }

}
