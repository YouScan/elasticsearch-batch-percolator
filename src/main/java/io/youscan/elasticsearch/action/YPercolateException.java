package io.youscan.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

/**
 * Exception during percolating document(s) at runtime.
 */
public class YPercolateException extends ElasticsearchException implements ElasticsearchWrapperException {

    private final ShardId shardId;

    public YPercolateException(ShardId shardId, String msg, Throwable cause) {
        super(msg, cause);
        Objects.requireNonNull(shardId, "shardId must not be null");
        this.shardId = shardId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public YPercolateException(StreamInput in) throws IOException {
        super(in);
        shardId = ShardId.readShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
    }
}