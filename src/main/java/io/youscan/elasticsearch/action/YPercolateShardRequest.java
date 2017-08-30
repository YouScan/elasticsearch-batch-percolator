package io.youscan.elasticsearch.action;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class YPercolateShardRequest extends BroadcastShardRequest {

    private String documentType;
    private BytesReference source;
    private BytesReference docSource;
    private boolean onlyCount;
    private int numberOfShards;
    private long startTime;

    public YPercolateShardRequest() {
    }

    YPercolateShardRequest(ShardId shardId, int numberOfShards, YPercolateRequest request) {
        super(shardId, request);
        this.documentType = request.documentType();
        this.source = request.source();
        this.docSource = request.docSource();
        this.onlyCount = request.onlyCount();
        this.numberOfShards = numberOfShards;
        this.startTime = request.startTime;
    }

    YPercolateShardRequest(ShardId shardId, OriginalIndices originalIndices) {
        super(shardId, originalIndices);
    }

    YPercolateShardRequest(ShardId shardId, YPercolateRequest request) {
        super(shardId, request);
        this.documentType = request.documentType();
        this.source = request.source();
        this.docSource = request.docSource();
        this.onlyCount = request.onlyCount();
        this.startTime = request.startTime;
    }

    public String documentType() {
        return documentType;
    }

    public BytesReference source() {
        return source;
    }

    public BytesReference docSource() {
        return docSource;
    }

    public boolean onlyCount() {
        return onlyCount;
    }

    void documentType(String documentType) {
        this.documentType = documentType;
    }

    void source(BytesReference source) {
        this.source = source;
    }

    void docSource(BytesReference docSource) {
        this.docSource = docSource;
    }

    void onlyCount(boolean onlyCount) {
        this.onlyCount = onlyCount;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    OriginalIndices originalIndices() {
        return originalIndices;
    }

    public long getStartTime() {
        return startTime;
    }

    void startTime(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        documentType = in.readString();
        source = in.readBytesReference();
        docSource = in.readBytesReference();
        onlyCount = in.readBoolean();
        numberOfShards = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(documentType);
        out.writeBytesReference(source);
        out.writeBytesReference(docSource);
        out.writeBoolean(onlyCount);
        out.writeVInt(numberOfShards);
    }
}
