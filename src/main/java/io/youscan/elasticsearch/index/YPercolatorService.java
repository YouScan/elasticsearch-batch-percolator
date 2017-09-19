package io.youscan.elasticsearch.index;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.meltwater.elasticsearch.index.RamDirectoryPercolatorIndex;
import io.youscan.elasticsearch.action.*;
import io.youscan.elasticsearch.shard.QueryAndSource;
import io.youscan.elasticsearch.shard.QueryMatch;
import io.youscan.elasticsearch.shard.YPercolatorQueriesRegistry;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.search.SearchService.DEFAULT_SEARCH_TIMEOUT;
import static org.elasticsearch.search.SearchService.NO_TIMEOUT;

public class YPercolatorService extends AbstractComponent {

    public final static String TYPE_NAME = "~ypercolator";

    private final IndicesService indicesService;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ClusterService clusterService;

    private final HighlightPhase highlightPhase;
    private final AggregationPhase aggregationPhase;
    private final ScriptService scriptService;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final ParseFieldMatcher parseFieldMatcher;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private QueryPhase queryPhase;
    private FetchPhase fetchPhase;

    private volatile TimeValue defaultSearchTimeout;

    private final SortParseElement sortParseElement;

    @Inject
    public YPercolatorService(
            Settings settings,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndicesService indicesService,
            PageCacheRecycler pageCacheRecycler,
            BigArrays bigArrays,
            HighlightPhase highlightPhase,
            ClusterService clusterService,
            ScriptService scriptService,
            MappingUpdatedAction mappingUpdatedAction,
            QueryPhase queryPhase,
            FetchPhase fetchPhase,
            AggregationPhase aggregationPhase
            ) {
        super(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
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
        this.aggregationPhase = aggregationPhase;

        this.sortParseElement = new SortParseElement();

        defaultSearchTimeout = settings.getAsTime(DEFAULT_SEARCH_TIMEOUT, NO_TIMEOUT);
    }

    public Iterable<PercolateResult> percolate(Iterable<Tuple<Integer, YPercolateShardRequest>> requestSlots, ShardId shardId) throws Throwable {

        // See PercolatorService.PercolatorType
        final byte percolatorTypeId = 0x03;

        ArrayList<Tuple<Integer, YPercolateShardRequest>> requests = Lists.newArrayList(requestSlots);

        long requestId = requestSlots.hashCode();
        IndexService percolateIndexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = percolateIndexService.shardSafe(shardId.getId());
        indexShard.readAllowed(); // check if we can read the shard...
        ConcurrentMap<String, QueryAndSource> percolateQueries = percolateIndexService.shardInjectorSafe(indexShard.shardId().id())
                .getInstance(YPercolatorQueriesRegistry.class)
                .percolateQueries();

        // TODO: The filteringAliases should be looked up at the coordinating node and serialized with all shard request,
        // just like is done in other apis.
        String[] filteringAliases = indexNameExpressionResolver.filteringAliases(
                clusterService.state(),
                indexShard.shardId().index().name(),
                shardId.getIndex()

        );
        Query aliasFilter = percolateIndexService.aliasesService().aliasFilter(filteringAliases);
        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(), shardId.getIndex(), shardId.id());

        Map<Integer, YPercolateContext> percolateContexts = createPercolateContexts(requests,
                searchShardTarget, indexShard, percolateIndexService,
                pageCacheRecycler, bigArrays, scriptService,
                aliasFilter, parseFieldMatcher);

        Directory directory = null;
        SearchContext searchContext = null;

        try {
            List<Tuple<Integer, ParsedDocument>> parsedDocuments =  parseRequests(percolateIndexService, requests, percolateContexts, shardId.getIndex());

            if (percolateQueries.isEmpty()) {
                List<PercolateResult> result = new ArrayList<>(requests.size());
                for(Tuple<Integer, YPercolateShardRequest> request: requests){
                    YPercolateShardResponse response = new YPercolateShardResponse(null, shardId.getIndex(), shardId.id(), percolateContexts.get(request.v1()));
                    PercolateResult resultItem = new PercolateResult(request.v1(), response);
                    result.add(resultItem);
                }
                return result;
            }

            List<Integer> nonParsedDocs = Lists.newArrayList();

            List<ParsedDocument> docs = new ArrayList<>(parsedDocuments.size());
            for (Tuple<Integer, ParsedDocument> doc: parsedDocuments){
                if(doc.v2() != null){
                    docs.add(doc.v2());
                } else {
                    nonParsedDocs.add(doc.v1());
                }
            }

            // We use a RAMDirectory here instead of a MemoryIndex.
            // In our tests MemoryIndex had worse indexing performance for normal sized quiddities.
            RamDirectoryPercolatorIndex index = new RamDirectoryPercolatorIndex(indexShard.mapperService());
            directory = index.indexDocuments(docs);

            searchContext = createSearchContext(shardId, percolateIndexService, indexShard, directory);

            long filteringStart = System.currentTimeMillis();
            Map<String, QueryAndSource> filteredQueries = filterQueriesToSearchWith(percolateQueries, directory);

            logger.debug("{}-{} Percolation queries filtered down to '{}' items in '{}' ms'.",
                    shardId,
                    requestId,
                    filteredQueries.size(),
                    System.currentTimeMillis() - filteringStart
            );

            Tuple<Map<Integer, YPercolateResponseItem>, Throwable> percolateResponsesResult = percolateResponses(searchContext, filteredQueries, parsedDocuments);
            Map<Integer, YPercolateResponseItem> responses = percolateResponsesResult.v1();
            List<PercolateResult> result = Lists.newArrayList();

            for (Tuple<Integer, YPercolateShardRequest> request: requests){
                int slot = request.v1();

                if(percolateResponsesResult.v2() == null){

                    YPercolateResponseItem item;

                    if(nonParsedDocs.contains(slot)){
                        item = new YPercolateResponseItem("_parse_error_");
                    } else {
                        item = responses.get(slot);
                    }

                    result.add(new PercolateResult(slot, new YPercolateShardResponse(item, shardId.getIndex(), shardId.id(), percolateContexts.get(slot))));
                } else {
                    result.add(new PercolateResult(slot, percolateResponsesResult.v2()));
                }
            }

            return result;

        } finally{
            for (YPercolateContext context: percolateContexts.values()){
                context.close();
            }

            if(directory != null)
                directory.close();

            if(searchContext != null)
                searchContext.close();

            percolateIndexService.cache().bitsetFilterCache().clear("Done percolating "+requestId);
            percolateIndexService.fieldData().clear();
            percolateIndexService.cache().clear("Done percolating "+requestId);
        }
    }

    private List<Tuple<Integer, ParsedDocument>> parseRequests(IndexService documentIndexService,
                                                               ArrayList<Tuple<Integer, YPercolateShardRequest>> requests,
                                                               Map<Integer, YPercolateContext> contexts, String index) {

        List<Tuple<Integer, ParsedDocument>> result = Lists.newArrayList();

        for (Tuple<Integer, YPercolateShardRequest> requestTuple: requests){

            Integer slot = requestTuple.v1();
            YPercolateShardRequest request = requestTuple.v2();
            YPercolateContext context = contexts.get(slot);

            BytesReference source = request.source();
            if (source == null || source.length() == 0) {
                return null;
            }

            // TODO: combine all feature parse elements into one map
            Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();
            Map<String, ? extends SearchParseElement> aggregationElements = aggregationPhase.parseElements();

            ParsedDocument doc = null;
            XContentParser parser = null;

            try{

                parser = XContentFactory.xContent(source).createParser(source);
                String currentFieldName = null;
                XContentParser.Token token;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        // we need to check the "doc" here, so the next token will be START_OBJECT which is
                        // the actual document starting
                        if ("doc".equals(currentFieldName)) {
                            if (doc != null) {
                                throw new ElasticsearchParseException("Either specify doc or get, not both");
                            }

                            MapperService mapperService = documentIndexService.mapperService();
                            DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                            SourceToParse sourceToParse = source(parser)
                                    .index(index)
                                    .type(request.documentType())
                                    .id(slot.toString())
                                    .flyweight(true);
                            doc = docMapper.getDocumentMapper().parse(sourceToParse);
                            if (docMapper.getMapping() != null) {
                                doc.addDynamicMappingsUpdate(docMapper.getMapping());
                            }
                            if (doc.dynamicMappingsUpdate() != null) {
                                mappingUpdatedAction.updateMappingOnMasterSynchronously(request.shardId().getIndex(), request.documentType(), doc.dynamicMappingsUpdate());
                            }

                            // the document parsing exists the "doc" object, so we need to set the new current field.
                            currentFieldName = parser.currentName();
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        SearchParseElement element = hlElements.get(currentFieldName);
                        if (element == null) {
                            element = aggregationElements.get(currentFieldName);
                        }

                        if ("query".equals(currentFieldName)) {
                            if (context.percolateQuery() != null) {
                                throw new ElasticsearchParseException("Either specify query or filter, not both");
                            }
                            context.percolateQuery(documentIndexService.queryParserService().parse(parser).query());
                        } else if ("filter".equals(currentFieldName)) {
                            if (context.percolateQuery() != null) {
                                throw new ElasticsearchParseException("Either specify query or filter, not both");
                            }
                            ParsedQuery parsedQuery = documentIndexService.queryParserService().parseInnerFilter(parser);
                            if(parsedQuery != null) {
                                context.percolateQuery(new ConstantScoreQuery(parsedQuery.query()));
                            }
                        } else if ("sort".equals(currentFieldName)) {
                            parseSort(parser, context);
                        } else if (element != null) {
                            element.parse(parser, context);
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if ("sort".equals(currentFieldName)) {
                            parseSort(parser, context);
                        }
                    } else if (token == null) {
                        break;
                    } else if (token.isValue()) {
                        if ("size".equals(currentFieldName)) {
                            context.size(parser.intValue());
                            if (context.size() < 0) {
                                throw new ElasticsearchParseException("size is set to [{}] and is expected to be higher or equal to 0", context.size());
                            }
                        } else if ("sort".equals(currentFieldName)) {
                            parseSort(parser, context);
                        } else if ("track_scores".equals(currentFieldName) || "trackScores".equals(currentFieldName)) {
                            context.trackScores(parser.booleanValue());
                        }
                    }
                }

                // We need to get the actual source from the request body for highlighting, so parse the request body again
                // and only get the doc source.
                // if (context.highlight() != null) {

                // Highlights are stored within the registered queries. So, we assuming we always need the source to be parsed
                if (true) {
                    parser.close();
                    currentFieldName = null;
                    parser = XContentFactory.xContent(source).createParser(source);
                    token = parser.nextToken();
                    assert token == XContentParser.Token.START_OBJECT;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("doc".equals(currentFieldName)) {
                                BytesStreamOutput bStream = new BytesStreamOutput();
                                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE, bStream);
                                builder.copyCurrentStructure(parser);
                                builder.close();
                                doc.setSource(bStream.bytes());
                                break;
                            } else {
                                parser.skipChildren();
                            }
                        } else if (token == null) {
                            break;
                        }
                    }
                }

            } catch (Throwable e) {
                logger.debug("failed to parse request", e);
                throw new ElasticsearchParseException("failed to parse request", e);
            } finally {
                if (parser != null) {
                    parser.close();
                }
            }

            result.add(Tuple.tuple(slot, doc));
        }

        return result;
    }

    private void parseSort(XContentParser parser, YPercolateContext context) throws Exception {
        sortParseElement.parse(parser, context);
        // null, means default sorting by relevancy
        if (context.sort() == null) {
            context.doSort = true;
        } else {
            throw new ElasticsearchParseException("Only _score desc is supported");
        }
    }

    private Map<Integer, YPercolateContext> createPercolateContexts(ArrayList<Tuple<Integer, YPercolateShardRequest>> requests,
                                                                    SearchShardTarget searchShardTarget, IndexShard indexShard,
                                                                    IndexService percolateIndexService,
                                                                    PageCacheRecycler pageCacheRecycler, BigArrays bigArrays,
                                                                    ScriptService scriptService, Query aliasFilter,
                                                                    ParseFieldMatcher parseFieldMatcher) {

        Map<Integer, YPercolateContext> result = Maps.newHashMap();
        for (Tuple<Integer, YPercolateShardRequest> requestTuple: requests){
            int slot = requestTuple.v1();
            YPercolateShardRequest request = requestTuple.v2();

            final YPercolateContext context = new YPercolateContext(
                    request, searchShardTarget, indexShard, percolateIndexService,
                    pageCacheRecycler, bigArrays, scriptService, aliasFilter, parseFieldMatcher
            );

            result.put(slot, context);
        }
        return result;
    }

    private Tuple<Map<Integer, YPercolateResponseItem>, Throwable> percolateResponses(SearchContext context, Map<String, QueryAndSource> percolateQueries, List<Tuple<Integer, ParsedDocument>> parsedDocuments) {

        Map<String, Integer> slotIds = Maps.newHashMap();
        Map<Integer, YPercolateResponseItem> responses = Maps.newHashMap();

        for(Tuple<Integer, ParsedDocument> document : parsedDocuments){
            String docId = document.v2().id();
            slotIds.put(docId, document.v1());

            // TODO Set all documents here
            IndexReader indexReader = context.searcher().getIndexReader();
            LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
            LeafSearchLookup leafLookup = context.lookup().getLeafSearchLookup(atomicReaderContext);
            leafLookup.setDocument(0);
            leafLookup.source().setSource(document.v2().source());

            responses.put(document.v1(), new YPercolateResponseItem(docId));
        }

        for (Map.Entry<String, QueryAndSource> entry : percolateQueries.entrySet()) {
            try{

                SearchContext.setCurrent(context);

                executeSearch(context, entry.getValue());
                for (SearchHit searchHit  : context.fetchResult().hits()) {
                    String id = searchHit.getId();

                    Integer slot = slotIds.get(id);
                    YPercolateResponseItem batchPercolateResponseItem = responses.get(slot);

                    QueryMatch queryMatch = getQueryMatch(entry, searchHit);
                    batchPercolateResponseItem.getMatches().put(queryMatch.getQueryId(), queryMatch);
                }
            }
            catch (Throwable e){
                logger.warn(
                        "Failed to execute query. Will not add it to matches. Query ID: {}, Query: {}: {} / '{}'",
                        e, entry.getKey(), entry.getValue().getQuery(), e.toString(), e.getMessage());
                return Tuple.tuple(responses, e);
            }
            finally{
                SearchContext.removeCurrent();
            }
        }

        return Tuple.tuple(responses, null);
    }

    private QueryMatch getQueryMatch(Map.Entry<String, QueryAndSource> entry, SearchHit searchHit) {
        QueryMatch queryMatch = new QueryMatch();
        queryMatch.setQueryId(entry.getKey());
        queryMatch.setHighlighs(searchHit.highlightFields());
        return queryMatch;
    }

    private void executeSearch(SearchContext context, QueryAndSource queryAndSource) {
        parseHighlighting(context, queryAndSource.getSource());

        Query q = queryAndSource.getQuery();
        ParsedQuery query = new ParsedQuery(q, ImmutableMap.<String, Query>of());
        context.parsedQuery(query);

        if (context.from() == -1) {
            context.from(0);
        }
        if (context.size() == -1) {
            context.size(Integer.MAX_VALUE);
        }

        queryPhase.preProcess(context);
        fetchPhase.preProcess(context);

        queryPhase.execute(context);
        setDocIdsToLoad(context);
        fetchPhase.execute(context);
    }

    private void setDocIdsToLoad(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        int totalSize = context.from() + context.size();
        int[] docIdsToLoad = new int[topDocs.totalHits];
        int counter = 0;
        for (int i = context.from(); i < totalSize; i++) {
            if (i < topDocs.scoreDocs.length) {
                docIdsToLoad[counter] = topDocs.scoreDocs[i].doc;
            } else {
                break;
            }
            counter++;
        }
        context.docIdsToLoad(docIdsToLoad, 0, counter);
    }

    //TODO do this when query is loaded into memory instead!
    private void parseHighlighting(SearchContext context, BytesReference source){
        XContentParser parser = null;
        Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = hlElements.get(fieldName);
                    if (element != null) {
                        element.parse(parser, context);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable ignore) {}
            throw new SearchParseException(context, "Failed to parse source [" + sSource + "]", null, e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private Map<String, QueryAndSource> filterQueriesToSearchWith(ConcurrentMap<String, QueryAndSource> percolateQueries, Directory directory) throws IOException {
        Map<String, QueryAndSource> filteredQueries = new HashMap<>();

        try(DirectoryReader reader = DirectoryReader.open(directory)){
            for(Map.Entry<String, QueryAndSource> entry:percolateQueries.entrySet()){
                try{
                    if(hasDocumentMatchingFilter(reader, entry.getValue().getLimitingFilter())){
                        filteredQueries.put(entry.getKey(), entry.getValue());
                    }
                } catch (Exception e){
                    logger.warn(
                            "Failed to pre-filter query. Assuming that it should be matched anyway. Query ID: {}, Filter: {}",
                            e, entry.getKey(), entry.getValue().getLimitingFilter());
                    filteredQueries.put(entry.getKey(), entry.getValue());
                }

            }
        }
        return filteredQueries;
    }

    private boolean hasDocumentMatchingFilter(IndexReader reader, Optional<Query> optionalFilter) throws IOException {
        if(optionalFilter.isPresent()){
            Query filter = optionalFilter.get();
            boolean found = false;
            // If you are not familiar with Lucene, this basically means that we try to
            // create an iterator for valid id:s for the filter for the given reader.
            // The filter and DocIdSet can both return null, to enable optimisations,
            // thus the null-checks. Null means that there were no matching docs, and
            // the same is true if the iterator refers to NO_MORE_DOCS immediately.
            for(LeafReaderContext leaf:reader.leaves()) {
                DocIdSet idSet = new QueryWrapperFilter(filter).getDocIdSet(leaf, leaf.reader().getLiveDocs());
                if (idSet != null) {
                    DocIdSetIterator iter = idSet.iterator();
                    if (iter != null && iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        found = true;
                        break;
                    }

                }
            }
            return found;
        }
        else{
            return true;
        }
    }

    private SearchContext createSearchContext(ShardId shardId,
                                              IndexService percolateIndexService,
                                              IndexShard indexShard,
                                              Directory directory) throws IOException {
        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(),
                shardId.getIndex(), shardId.id());

        ShardSearchLocalRequest shardSearchLocalRequest = new ShardSearchLocalRequest(
                new ShardId("local_index", 0), 0, SearchType.QUERY_AND_FETCH, null, null, false);
        DocSearcher docSearcher = new DocSearcher(new IndexSearcher(DirectoryReader.open(directory)));
        Counter counter = Counter.newCounter();

        return new DefaultSearchContext(
                0,
                shardSearchLocalRequest,
                searchShardTarget,
                docSearcher,
                percolateIndexService,
                indexShard,
                scriptService,
                pageCacheRecycler,
                bigArrays,
                counter,
                parseFieldMatcher,
                defaultSearchTimeout
        );
    }

    private class DocSearcher extends Engine.Searcher {

        private DocSearcher(IndexSearcher searcher) {
            super("percolate", searcher);
        }

        @Override
        public void close() throws ElasticsearchException {
            try {
                this.reader().close();
            } catch (IOException e) {
                throw new ElasticsearchException("failed to close IndexReader in batch percolator", e);
            }
        }

    }

    public class PercolateResult{
        private int slot;
        private YPercolateShardResponse response;
        private Throwable error;

        public PercolateResult(int slot, YPercolateShardResponse response){
            this.slot = slot;
            this.response = response;
        }

        public PercolateResult(int slot, Throwable error){
            this.slot = slot;
            this.error = error;
        }

        public boolean isError(){
            return error != null;
        }

        public int getSlot(){
            return slot;
        }

        public YPercolateShardResponse getResponse(){
            return response;
        }

        public Throwable getError() {
            return error;
        }
    }

    // See TransportBatchPercolateAction#mergeResults(BatchPercolateRequest request, AtomicReferenceArray shardsResponses) {
    public YPercolateResponse reduce(YPercolateRequest request, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;

        List<YPercolateShardResponse> shardResults = newArrayList();
        List<ShardOperationFailedException> shardFailures = null;

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                YPercolateShardResponse batchPercolateShardResponse = (YPercolateShardResponse) shardResponse;
                successfulShards++;
                if (!batchPercolateShardResponse.isEmpty()) {
                    shardResults.add(batchPercolateShardResponse);
                }
            }
        }

        long tookInMillis = System.currentTimeMillis() - request.startTime;
        if (shardResults.isEmpty()) {
            return new YPercolateResponse(
                    new ArrayList<YPercolateResponseItem>(), tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures
            );
        } else {
            YPercolateResponseItem mergedItem = new YPercolateResponseItem();

            for(YPercolateShardResponse response : shardResults){
                YPercolateResponseItem item = response.getItem();
                mergedItem.getMatches().putAll(item.getMatches());
                mergedItem.setDocId(item.getDocId());
            }

            List<YPercolateResponseItem> listItems = newArrayList(new YPercolateResponseItem[]{ mergedItem });
            return new YPercolateResponse(
                listItems, tookInMillis, shardsResponses.length(), successfulShards, failedShards, shardFailures
            );
        }
    }
}
