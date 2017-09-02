package io.youscan.elasticsearch.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.Index;

/**
 * Exception during parsing a percolator query.
 */
public class YPercolatorQueryException extends ElasticsearchException {

    public YPercolatorQueryException(Index index, String msg) {
        super(msg, index);
    }

    public YPercolatorQueryException(Index index, String msg, Throwable cause) {
        super(msg, cause, index);
    }
}
