package io.youscan.elasticsearch.shard;

import com.google.common.base.Optional;
import com.meltwater.elasticsearch.index.queries.LimitingFilterFactory;
import io.youscan.elasticsearch.index.YPercolatorService;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentTypeListener;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each shard will have a percolator registry even if there isn't a {@link YPercolatorService#TYPE_NAME} document type in the index.
 * For shards with indices that have no {@link YPercolatorService#TYPE_NAME} document type, this will hold no percolate queries.
 * <p>
 * Once a document type has been created, the real-time percolator will start to listen to write events and update the
 * this registry with queries in real time.
 * </p>
 */
public class YPercolatorQueriesRegistry extends AbstractIndexShardComponent implements IndexComponent, Closeable {

    // This is a shard level service, but these below are index level service:
    private final IndexQueryParserService queryParserService;
    private final MapperService mapperService;
    private final IndicesLifecycle indicesLifecycle;
    private final IndexCache indexCache;
    private final IndexFieldDataService indexFieldDataService;
    private final LimitingFilterFactory limitingFilterFactory;

    private final ShardIndexingService indexingService;
    private final ConcurrentMap<String, QueryAndSource> percolateQueries = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final YPercolatorQueriesRegistry.ShardLifecycleListener shardLifecycleListener = new YPercolatorQueriesRegistry.ShardLifecycleListener();
    private final YPercolatorQueriesRegistry.PercolateTypeListener percolateTypeListener = new YPercolatorQueriesRegistry.PercolateTypeListener();

    private final YPercolatorQueriesRegistry.RealTimePercolatorOperationListener realTimePercolatorOperationListener = new YPercolatorQueriesRegistry.RealTimePercolatorOperationListener();

    private final AtomicBoolean realTimePercolatorEnabled = new AtomicBoolean(false);

    @Inject
    public YPercolatorQueriesRegistry(ShardId shardId, Settings indexSettings, IndexQueryParserService queryParserService,
                                          IndexShard indexShard, IndicesLifecycle indicesLifecycle, MapperService mapperService,
                                          IndexCache indexCache, IndexFieldDataService indexFieldDataService) {
        super(shardId, indexSettings);
        this.queryParserService = queryParserService;
        this.mapperService = mapperService;
        this.indicesLifecycle = indicesLifecycle;
        this.indexingService = indexShard.indexingService();
        this.indexCache = indexCache;
        this.indexFieldDataService = indexFieldDataService;

        indicesLifecycle.addListener(shardLifecycleListener);
        mapperService.addTypeListener(percolateTypeListener);

        this.limitingFilterFactory = new LimitingFilterFactory();
    }

    public ConcurrentMap<String, QueryAndSource> percolateQueries() {
        return percolateQueries;
    }

    @Override
    public void close() {
        mapperService.removeTypeListener(percolateTypeListener);
        indicesLifecycle.removeListener(shardLifecycleListener);
        indexingService.removeListener(realTimePercolatorOperationListener);
        clear();
    }

    @Override
    public Index index() {
        return shardId.index();
    }

    public void clear() {
        percolateQueries.clear();
    }

    void enableRealTimePercolator() {
        if (realTimePercolatorEnabled.compareAndSet(false, true)) {
            indexingService.addListener(realTimePercolatorOperationListener);
        }
    }

    public void addPercolateQuery(String idAsString, BytesReference source) {
        QueryAndSource newQuery = parsePercolatorDocument(idAsString, source);
        percolateQueries.put(idAsString, newQuery);
    }

    public void removePercolateQuery(String idAsString) {
        percolateQueries.remove(idAsString);
    }

    QueryAndSource parsePercolatorDocument(String id, BytesReference source) {
        String type = null;
        BytesReference querySource = null;

        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(source);
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchException("failed to parse query [" + id + "], not starting with OBJECT");
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        if (type != null) {
                            Query query = parseQuery(type, null, parser);
                            return new QueryAndSource(query, limitingFilterFactory.limitingFilter(query), source);
                        } else {
                            XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                            builder.copyCurrentStructure(parser);
                            querySource = builder.bytes();
                            builder.close();
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if (token.isValue()) {
                    if ("type".equals(currentFieldName)) {
                        type = parser.text();
                    }
                }
            }
            Query query = parseQuery(type, querySource, null);
            Optional<Query> queryOptional = limitingFilterFactory.limitingFilter(query);
            return new QueryAndSource(query, queryOptional, source);
        } catch (Exception e) {
            throw new YPercolatorQueryException(shardId().index(), "failed to parse query [" + id + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private Query parseQuery(String type, BytesReference querySource, XContentParser parser) {
        if (type == null) {
            if (parser != null) {
                return queryParserService.parse(parser).query();
            } else {
                return queryParserService.parse(querySource).query();
            }
        }

        String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{type});
        try {
            if (parser != null) {
                return queryParserService.parse(parser).query();
            } else {
                return queryParserService.parse(querySource).query();
            }
        } finally {
            QueryParseContext.setTypes(previousTypes);
        }
    }

    private class PercolateTypeListener implements DocumentTypeListener {

        @Override
        public void beforeCreate(DocumentMapper mapper) {
            if (YPercolatorService.TYPE_NAME.equals(mapper.type())) {
                enableRealTimePercolator();
            }
        }
    }

    private class ShardLifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            if (hasPercolatorType(indexShard)) {
                enableRealTimePercolator();
            }
        }

        @Override
        public void beforeIndexShardPostRecovery(IndexShard indexShard) {
            if (hasPercolatorType(indexShard)) {
                // percolator index has started, fetch what we can from it and initialize the indices
                // we have
                //TODO lower to debug level
                logger.info("loading percolator queries for index [{}] and shard[{}]...", shardId.index(), shardId.id());
                loadQueries(indexShard);
                logger.info("done loading percolator queries for index [{}] and shard[{}], nr of queries: [{}]", shardId.index(), shardId.id(), percolateQueries.size());
            }
        }

        private boolean hasPercolatorType(IndexShard indexShard) {
            ShardId otherShardId = indexShard.shardId();
            return shardId.equals(otherShardId) && mapperService.hasMapping(YPercolatorService.TYPE_NAME);
        }

        private void loadQueries(IndexShard shard) {
            try {
                shard.refresh("percolator_load_queries");
                // Maybe add a mode load? This isn't really a write. We need write b/c state=post_recovery
                try(Engine.Searcher searcher = shard.engine().acquireSearcher("percolator_load_queries")) {

                    Query query = new ConstantScoreQuery(
                            indexCache.query().doCache(

                                    new TermQuery(new Term(TypeFieldMapper.NAME, YPercolatorService.TYPE_NAME))
                                            .createWeight(searcher.searcher(), false),

                                    new UsageTrackingQueryCachingPolicy()
                            ).getQuery()
                    );
                    YQueriesLoaderCollector queryCollector = new YQueriesLoaderCollector(YPercolatorQueriesRegistry.this, logger, mapperService, indexFieldDataService);
                    searcher.searcher().search(query, queryCollector);
                    Map<String, QueryAndSource> queries = queryCollector.queries();
                    for(Map.Entry<String, QueryAndSource> entry : queries.entrySet()) {
                        percolateQueries.put(entry.getKey(), entry.getValue());
                    }
                }
            } catch (Exception e) {
                throw new YPercolatorQueryException(shardId.index(), "failed to load queries from percolator index", e);
            }
        }

    }

    private class RealTimePercolatorOperationListener extends IndexingOperationListener {

        @Override
        public Engine.Create preCreate(Engine.Create create) {
            // validate the query here, before we index
            if (YPercolatorService.TYPE_NAME.equals(create.type())) {
                parsePercolatorDocument(create.id(), create.source());
            }
            return create;
        }

        @Override
        public void postCreateUnderLock(Engine.Create create) {
            // add the query under a doc lock
            if (YPercolatorService.TYPE_NAME.equals(create.type())) {
                addPercolateQuery(create.id(), create.source());
            }
        }

        @Override
        public Engine.Index preIndex(Engine.Index index) {
            // validate the query here, before we index
            if (YPercolatorService.TYPE_NAME.equals(index.type())) {
                parsePercolatorDocument(index.id(), index.source());
            }
            return index;
        }

        @Override
        public void postIndexUnderLock(Engine.Index index) {
            // add the query under a doc lock
            if (YPercolatorService.TYPE_NAME.equals(index.type())) {
                addPercolateQuery(index.id(), index.source());
            }
        }

        @Override
        public void postDeleteUnderLock(Engine.Delete delete) {
            // remove the query under a lock
            if (YPercolatorService.TYPE_NAME.equals(delete.type())) {
                removePercolateQuery(delete.id());
            }
        }
    }

}
