package io.youscan.elasticsearch.action;

import io.youscan.elasticsearch.index.YPercolatorService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.percolate.TransportShardMultiPercolateAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransportShardMultiYPercolateAction  extends TransportSingleShardAction<TransportShardMultiYPercolateAction.Request, TransportShardMultiYPercolateAction.Response> {

    private final YPercolatorService percolatorService;

    private static final String ACTION_NAME = MultiYPercolateAction.NAME + "[shard]";

    @Inject
    public TransportShardMultiYPercolateAction(
            Settings settings,
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            YPercolatorService percolatorService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
                settings,
                ACTION_NAME,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                TransportShardMultiYPercolateAction.Request.class,
                ThreadPool.Names.PERCOLATE);

        this.percolatorService = percolatorService;
    }


    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected TransportShardMultiYPercolateAction.Response newResponse() {
        return new TransportShardMultiYPercolateAction.Response();
    }

    @Override
    protected boolean resolveIndex(TransportShardMultiYPercolateAction.Request request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting().getShards(
                state, request.concreteIndex(), request.request().shardId(), request.request().preference
        );
    }

    @Override
    protected TransportShardMultiYPercolateAction.Response shardOperation(TransportShardMultiYPercolateAction.Request request, ShardId shardId) {

        ArrayList<Tuple<Integer, YPercolateShardRequest>> requests = new ArrayList<>(1);
        for (TransportShardMultiYPercolateAction.Request.Item item : request.items) {
            requests.add(Tuple.tuple(item.slot(), item.request()));
        }

        TransportShardMultiYPercolateAction.Response response = new TransportShardMultiYPercolateAction.Response();
        response.items = new ArrayList<>(request.items.size());

        Iterable<YPercolatorService.PercolateResult> responses = null;
        Throwable error = null;

        try {
            responses = percolatorService.percolate(requests, shardId);
        } catch (Throwable e) {
            if (TransportActions.isShardNotAvailableException(e)) {
                throw (ElasticsearchException) e;
            } else{
                logger.debug("{} failed to multi percolate", e, request.shardId());
                error = e;
            }
        }

        if (error != null){
            for (Request.Item item : request.items) {
                Response.Item responseItem = new Response.Item(item.slot, error);
                response.items.add(responseItem);
            }
        } else{
            Response.Item responseItem;
            for (YPercolatorService.PercolateResult x: responses){
                if(x.isError()){
                    responseItem = new Response.Item(x.getSlot(), x.getError());
                } else{
                    responseItem = new Response.Item(x.getSlot(), x.getResponse());
                }
                response.items.add(responseItem);
            }
        }

        return response;
    }


    public static class Request extends SingleShardRequest implements IndicesRequest {

        private int shardId;
        private String preference;
        private List<TransportShardMultiYPercolateAction.Request.Item> items;

        public Request() {
        }

        Request(MultiYPercolateRequest multiPercolateRequest, String concreteIndex, int shardId, String preference) {
            super(multiPercolateRequest, concreteIndex);
            this.shardId = shardId;
            this.preference = preference;
            this.items = new ArrayList<>();
        }

        @Override
        public ActionRequestValidationException validate() {
            return super.validateNonNullIndex();
        }

        @Override
        public String[] indices() {
            List<String> indices = new ArrayList<>();
            for (TransportShardMultiYPercolateAction.Request.Item item : items) {
                Collections.addAll(indices, item.request.indices());
            }
            return indices.toArray(new String[indices.size()]);
        }

        public int shardId() {
            return shardId;
        }

        public void add(TransportShardMultiYPercolateAction.Request.Item item) {
            items.add(item);
        }

        public List<TransportShardMultiYPercolateAction.Request.Item> items() {
            return items;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = in.readVInt();
            preference = in.readOptionalString();
            int size = in.readVInt();
            items = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int slot = in.readVInt();
                OriginalIndices originalIndices = OriginalIndices.readOriginalIndices(in);
                YPercolateShardRequest shardRequest = new YPercolateShardRequest(new ShardId(index, shardId), originalIndices);
                shardRequest.documentType(in.readString());
                shardRequest.source(in.readBytesReference());
                shardRequest.docSource(in.readBytesReference());
                shardRequest.onlyCount(in.readBoolean());
                if (in.getVersion().onOrAfter(Version.V_2_3_0)) {
                    shardRequest.startTime(in.readLong());
                }
                TransportShardMultiYPercolateAction.Request.Item item = new TransportShardMultiYPercolateAction.Request.Item(slot, shardRequest);
                items.add(item);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shardId);
            out.writeOptionalString(preference);
            out.writeVInt(items.size());
            for (TransportShardMultiYPercolateAction.Request.Item item : items) {
                out.writeVInt(item.slot);
                OriginalIndices.writeOriginalIndices(item.request.originalIndices(), out);
                out.writeString(item.request.documentType());
                out.writeBytesReference(item.request.source());
                out.writeBytesReference(item.request.docSource());
                out.writeBoolean(item.request.onlyCount());
                if (out.getVersion().onOrAfter(Version.V_2_3_0)) {
                    out.writeLong(item.request.getStartTime());
                }
            }
        }


        static class Item {

            private final int slot;
            private final YPercolateShardRequest request;

            public Item(int slot, YPercolateShardRequest request) {
                this.slot = slot;
                this.request = request;
            }

            public int slot() {
                return slot;
            }

            public YPercolateShardRequest request() {
                return request;
            }

        }
    }

    public static class Response extends ActionResponse {

        private List<Item> items;

        public List<Item> items() {
            return items;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(items.size());
            for (TransportShardMultiYPercolateAction.Response.Item item : items) {
                out.writeVInt(item.slot);
                if (item.response != null) {
                    out.writeBoolean(true);
                    item.response.writeTo(out);
                } else {
                    out.writeBoolean(false);
                    out.writeThrowable(item.error);
                }
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int size = in.readVInt();
            items = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int slot = in.readVInt();
                if (in.readBoolean()) {
                    YPercolateShardResponse shardResponse = new YPercolateShardResponse();
                    shardResponse.readFrom(in);
                    items.add(new TransportShardMultiYPercolateAction.Response.Item(slot, shardResponse));
                } else {
                    items.add(new TransportShardMultiYPercolateAction.Response.Item(slot, in.readThrowable()));
                }
            }
        }

        public static class Item {

            private final int slot;
            private final YPercolateShardResponse response;
            private final Throwable error;

            public Item(Integer slot, YPercolateShardResponse response) {
                this.slot = slot;
                this.response = response;
                this.error = null;
            }

            public Item(Integer slot, Throwable error) {
                this.slot = slot;
                this.error = error;
                this.response = null;
            }

            public int slot() {
                return slot;
            }

            public YPercolateShardResponse response() {
                return response;
            }

            public Throwable error() {
                return error;
            }

            public boolean failed() {
                return error != null;
            }
        }

    }
}
