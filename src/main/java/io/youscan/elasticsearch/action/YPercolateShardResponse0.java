package io.youscan.elasticsearch.action;

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

public class YPercolateShardResponse0 extends BroadcastShardResponse {

    private static final BytesRef[] EMPTY_MATCHES = new BytesRef[0];
    private static final float[] EMPTY_SCORES = new float[0];
    private static final List<Map<String, HighlightField>> EMPTY_HL = Collections.emptyList();

    private long count;
    private float[] scores;
    private BytesRef[] matches;
    private List<Map<String, HighlightField>> hls;
    private byte percolatorTypeId;
    private int requestedSize;

    private InternalAggregations aggregations;
    private List<SiblingPipelineAggregator> pipelineAggregators;

    YPercolateShardResponse0() {
        hls = new ArrayList<>();
    }

    public YPercolateShardResponse0(BytesRef[] matches, List<Map<String, HighlightField>> hls, long count, float[] scores, PercolateContext context, ShardId shardId) {
        super(shardId);
        this.matches = matches;
        this.hls = hls;
        this.count = count;
        this.scores = scores;
        this.percolatorTypeId = context.percolatorTypeId;
        this.requestedSize = context.size();
        QuerySearchResult result = context.queryResult();
        if (result != null) {
            if (result.aggregations() != null) {
                this.aggregations = (InternalAggregations) result.aggregations();
            }
            this.pipelineAggregators = result.pipelineAggregators();
        }
    }

    public YPercolateShardResponse0(BytesRef[] matches, long count, float[] scores, PercolateContext context, ShardId shardId) {
        this(matches, EMPTY_HL, count, scores, context, shardId);
    }

    public YPercolateShardResponse0(BytesRef[] matches, List<Map<String, HighlightField>> hls, long count, PercolateContext context, ShardId shardId) {
        this(matches, hls, count, EMPTY_SCORES, context, shardId);
    }

    public YPercolateShardResponse0(long count, PercolateContext context, ShardId shardId) {
        this(EMPTY_MATCHES, EMPTY_HL, count, EMPTY_SCORES, context, shardId);
    }

    public YPercolateShardResponse0(PercolateContext context, ShardId shardId) {
        this(EMPTY_MATCHES, EMPTY_HL, 0, EMPTY_SCORES, context, shardId);
    }

    public BytesRef[] matches() {
        return matches;
    }

    public float[] scores() {
        return scores;
    }

    public long count() {
        return count;
    }

    public int requestedSize() {
        return requestedSize;
    }

    public List<Map<String, HighlightField>> hls() {
        return hls;
    }

    public InternalAggregations aggregations() {
        return aggregations;
    }

    public List<SiblingPipelineAggregator> pipelineAggregators() {
        return pipelineAggregators;
    }

    public byte percolatorTypeId() {
        return percolatorTypeId;
    }

    public boolean isEmpty() {
        return percolatorTypeId == 0x00;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        percolatorTypeId = in.readByte();
        requestedSize = in.readVInt();
        count = in.readVLong();
        matches = new BytesRef[in.readVInt()];
        for (int i = 0; i < matches.length; i++) {
            matches[i] = in.readBytesRef();
        }
        scores = new float[in.readVInt()];
        for (int i = 0; i < scores.length; i++) {
            scores[i] = in.readFloat();
        }
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            int mSize = in.readVInt();
            Map<String, HighlightField> fields = new HashMap<>();
            for (int j = 0; j < mSize; j++) {
                fields.put(in.readString(), HighlightField.readHighlightField(in));
            }
            hls.add(fields);
        }
        aggregations = InternalAggregations.readOptionalAggregations(in);
        if (in.readBoolean()) {
            int pipelineAggregatorsSize = in.readVInt();
            List<SiblingPipelineAggregator> pipelineAggregators = new ArrayList<>(pipelineAggregatorsSize);
            for (int i = 0; i < pipelineAggregatorsSize; i++) {
                BytesReference type = in.readBytesReference();
                PipelineAggregator pipelineAggregator = PipelineAggregatorStreams.stream(type).readResult(in);
                pipelineAggregators.add((SiblingPipelineAggregator) pipelineAggregator);
            }
            this.pipelineAggregators = pipelineAggregators;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(percolatorTypeId);
        out.writeVLong(requestedSize);
        out.writeVLong(count);
        out.writeVInt(matches.length);
        for (BytesRef match : matches) {
            out.writeBytesRef(match);
        }
        out.writeVLong(scores.length);
        for (float score : scores) {
            out.writeFloat(score);
        }
        out.writeVInt(hls.size());
        for (Map<String, HighlightField> hl : hls) {
            out.writeVInt(hl.size());
            for (Map.Entry<String, HighlightField> entry : hl.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
        out.writeOptionalStreamable(aggregations);
        if (pipelineAggregators == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(pipelineAggregators.size());
            for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
                out.writeBytesReference(pipelineAggregator.type().stream());
                pipelineAggregator.writeTo(out);
            }
        }
    }
}
