package io.youscan.elasticsearch.index;

import io.youscan.elasticsearch.action.YPercolateRequest;
import io.youscan.elasticsearch.action.YPercolateResponse;
import io.youscan.elasticsearch.action.YPercolateShardRequest;
import io.youscan.elasticsearch.action.YPercolateShardResponse;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.SearchService.DEFAULT_SEARCH_TIMEOUT;
import static org.elasticsearch.search.SearchService.NO_TIMEOUT;

public class YPercolatorService extends AbstractComponent {

    public final static String TYPE_NAME = "~ypercolator";

    private final IndicesService indicesService;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ClusterService clusterService;

    private final HighlightPhase highlightPhase;
    private final ScriptService scriptService;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final ParseFieldMatcher parseFieldMatcher;
    private QueryPhase queryPhase;
    private FetchPhase fetchPhase;

    private volatile TimeValue defaultSearchTimeout;

    @Inject
    public YPercolatorService(Settings settings, IndicesService indicesService,
                                  PageCacheRecycler pageCacheRecycler, BigArrays bigArrays,
                                  HighlightPhase highlightPhase, ClusterService clusterService,
                                  ScriptService scriptService,
                                  MappingUpdatedAction mappingUpdatedAction,
                                  QueryPhase queryPhase,
                                  FetchPhase fetchPhase) {
        super(settings);
        this.indicesService = indicesService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.bigArrays = bigArrays;
        this.clusterService = clusterService;
        this.highlightPhase = highlightPhase;
        this.scriptService = scriptService;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;

        defaultSearchTimeout = settings.getAsTime(DEFAULT_SEARCH_TIMEOUT, NO_TIMEOUT);
    }

    public YPercolatorService.ReduceResult reduce(byte percolatorTypeId, List<YPercolateShardResponse> shardResults, YPercolateRequest request) {
        return null;
    }

    public YPercolateShardResponse percolate(YPercolateShardRequest request)  throws IOException {
        // TODO
        // public BatchPercolateShardResponse percolate(BatchPercolateShardRequest request) throws IOException
        return null;
    }

    public final static class ReduceResult {

        private final long count;
        private final YPercolateResponse.Match[] matches;
        private final InternalAggregations reducedAggregations;

        ReduceResult(long count, YPercolateResponse.Match[] matches, InternalAggregations reducedAggregations) {
            this.count = count;
            this.matches = matches;
            this.reducedAggregations = reducedAggregations;
        }

        public ReduceResult(long count, InternalAggregations reducedAggregations) {
            this.count = count;
            this.matches = null;
            this.reducedAggregations = reducedAggregations;
        }

        public long count() {
            return count;
        }

        public YPercolateResponse.Match[] matches() {
            return matches;
        }

        public InternalAggregations reducedAggregations() {
            return reducedAggregations;
        }
    }
}
