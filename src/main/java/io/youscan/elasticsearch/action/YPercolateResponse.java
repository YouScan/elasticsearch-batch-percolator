package io.youscan.elasticsearch.action;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.action.support.RestActions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class YPercolateResponse extends BroadcastResponse implements Streamable, Iterable<YPercolateResponseItem>, ToXContent {

    private List<YPercolateResponseItem> results;
    private long tookMs;

    public YPercolateResponse(){}

    public YPercolateResponse(List<YPercolateResponseItem> results,
                              long tookInMillis,
                              int totalShards,
                              int successfulShards,
                              int failedShards,
                              List<ShardOperationFailedException> shardFailures) {

        super(totalShards, successfulShards, failedShards, shardFailures);
        this.results = results;
        this.tookMs = tookInMillis;
    }

    public List<YPercolateResponseItem> getResults() {
        return this.results;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookMs = in.readVLong();
        int size = in.readVInt();
        this.results= new ArrayList<>(size);
        for(int i = 0; i < size; i++){
            YPercolateResponseItem item = new YPercolateResponseItem();
            item.readFrom(in);
            results.add(item);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookMs);
        out.writeVInt(results.size());
        for (YPercolateResponseItem item : results) {
            item.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(YPercolateResponse.Fields.TOOK, tookMs);
        RestActions.buildBroadcastShardsHeader(builder, params, this);

        builder.startArray("results");
        for (YPercolateResponseItem result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public Iterator<YPercolateResponseItem> iterator() {
        return results.iterator();
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
    }

}
