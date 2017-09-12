package io.youscan.elasticsearch.action;

import com.google.common.collect.Maps;
import com.meltwater.elasticsearch.action.BatchPercolateResponseItem;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.percolator.PercolateContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.*;


public class YPercolateShardResponse extends BroadcastShardResponse {

    YPercolateResponseItem item;
    private byte percolatorTypeId;

    public YPercolateShardResponse(){

    }
    public YPercolateShardResponse(YPercolateResponseItem item, String index, int shardId, byte percolatorTypeId) {
        super(new ShardId(index, shardId));
        this.item = item;
        this.percolatorTypeId = percolatorTypeId;
    }

    public YPercolateResponseItem getItem() {
        return item;
    }

    public boolean isEmpty() {
        return item == null || item.getMatches().size() == 0;
    }

    public byte percolatorTypeId() {
        return percolatorTypeId;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        item = new YPercolateResponseItem();
        item.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        item.writeTo(out);
    }
}
