package io.youscan.elasticsearch.shard;

import com.google.common.base.Optional;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A Query used for percolation, along with source (and relevant metadata).
 */
public class QueryAndSource {
    final Query query;
    final Optional<Query> limitingFilter;
    final BytesReference source;

    public QueryAndSource(Query query, Optional<Query> limitingFilter, BytesReference source) {
        this.query = query;
        this.limitingFilter = limitingFilter;
        this.source = source;
    }

    public Query getQuery() {
        return query;
    }

    public Optional<Query> getLimitingFilter() {
        return limitingFilter;
    }

    public BytesReference getSource() {
        return source;
    }

}