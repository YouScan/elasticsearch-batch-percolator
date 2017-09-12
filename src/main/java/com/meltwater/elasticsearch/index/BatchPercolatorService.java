/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.meltwater.elasticsearch.index;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.meltwater.elasticsearch.action.BatchPercolateResponseItem;
import com.meltwater.elasticsearch.action.BatchPercolateShardRequest;
import com.meltwater.elasticsearch.action.BatchPercolateShardResponse;
import com.meltwater.elasticsearch.shard.BatchPercolatorQueriesRegistry;
import com.meltwater.elasticsearch.shard.QueryAndSource;
import com.meltwater.elasticsearch.shard.QueryMatch;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.search.SearchService.DEFAULT_SEARCH_TIMEOUT;
import static org.elasticsearch.search.SearchService.NO_TIMEOUT;

/**
 * For each request, the {@link BatchPercolatorService#percolate} function is
 * called on each shard which contains percolation queries.
 *
 * The percolation is then executed in the following way:
 * 1. Create a RAMDirectory and index all documents in the request to it
 * 2. For each query:
 *  2.1 Parse highlighting options and set it to the search context (a class which is extremely stateful).
 *  2.2 Execute the query and collect results
 * 3. Send back the matches for each documents in a BatchPercolateShardResponse.
 */
public class BatchPercolatorService extends AbstractComponent {
    public final static String TYPE_NAME = "~batchpercolator";

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
    public BatchPercolatorService(Settings settings, IndicesService indicesService,
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

    public BatchPercolateShardResponse percolate(BatchPercolateShardRequest request) throws IOException {
        long requestId = request.hashCode();
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = percolateIndexService.shardSafe(request.shardId().getId());
        indexShard.readAllowed(); // check if we can read the shard...
        ConcurrentMap<String, QueryAndSource> percolateQueries = percolateIndexService.shardInjectorSafe(indexShard.shardId().id())
                .getInstance(BatchPercolatorQueriesRegistry.class)
                .percolateQueries();

        List<ParsedDocument> parsedDocuments = parseRequest(percolateIndexService, request);
        if (percolateQueries.isEmpty()) {
            return new BatchPercolateShardResponse(emptyPercolateResponses(parsedDocuments),
                    request.shardId().getIndex(), request.shardId().id());
        } else if (parsedDocuments == null) {
            throw new ElasticsearchException("Nothing to percolate");
        }

        // We use a RAMDirectory here instead of a MemoryIndex.
        // In our tests MemoryIndex had worse indexing performance for normal sized quiddities.
        RamDirectoryPercolatorIndex index = new RamDirectoryPercolatorIndex(indexShard.mapperService());
        Directory directory = index.indexDocuments(parsedDocuments);

        SearchContext context = createSearchContext(request, percolateIndexService, indexShard, directory);

        long filteringStart = System.currentTimeMillis();
        Map<String, QueryAndSource> filteredQueries = filterQueriesToSearchWith(percolateQueries, directory);

        logger.debug("{}-{} Percolation queries filtered down to '{}' items in '{}' ms'.",
                request.shardId(),
                requestId,
                filteredQueries.size(),
                System.currentTimeMillis() - filteringStart
        );
        //Perform the actual matching
        Map<String, BatchPercolateResponseItem> responses = percolateResponses(context, filteredQueries, parsedDocuments);

        directory.close();
        context.close();
        percolateIndexService.cache().bitsetFilterCache().clear("Done percolating "+requestId);
        percolateIndexService.fieldData().clear();
        percolateIndexService.cache().clear("Done percolating "+requestId);
        return new BatchPercolateShardResponse(responses, request.shardId().getIndex(), request.shardId().id());
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

    private SearchContext createSearchContext(BatchPercolateShardRequest request,
                                              IndexService percolateIndexService,
                                              IndexShard indexShard,
                                              Directory directory) throws IOException {
        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(),
                request.shardId().getIndex(), request.shardId().id());

        ShardSearchLocalRequest shardSearchLocalRequest = new ShardSearchLocalRequest(new ShardId("local_index", 0), 0, SearchType.QUERY_AND_FETCH, null, null, false);
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

    private Map<String, BatchPercolateResponseItem> emptyPercolateResponses(List<ParsedDocument> parsedDocuments) {
        Map<String, BatchPercolateResponseItem> items = Maps.newHashMap();
        for(ParsedDocument document : parsedDocuments){
            items.put(document.id(),
                    new BatchPercolateResponseItem(Maps.<String, QueryMatch>newHashMap(), document.id()));
        }
        return items;
    }

    private List<ParsedDocument> parseRequest(IndexService documentIndexService, BatchPercolateShardRequest request) throws ElasticsearchException {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            return null;
        }

        List<ParsedDocument> docs = new ArrayList<>();
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    if("docs".equals(parser.currentName())){
                        docs.addAll(parsedDocuments(documentIndexService, request, parser));
                    }
                }
            }
        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        return docs;
    }

    private List<ParsedDocument> parsedDocuments(IndexService documentIndexService,
                                                 BatchPercolateShardRequest request,
                                                 XContentParser parser) throws Throwable  {
        List<ParsedDocument> docs = new ArrayList<>();

        parser.nextToken();
        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            MapperService mapperService = documentIndexService.mapperService();
            DocumentMapperForType docMapperTuple = mapperService.documentMapperWithAutoCreate(request.documentType());

            BytesStreamOutput buffer = new BytesStreamOutput();
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, buffer);
            builder.copyCurrentStructure(parser);
            builder.close();
            docs.add(getParsedDocument(request, docMapperTuple, buffer));
            buffer.close();
        }

        return docs;
    }

    private ParsedDocument getParsedDocument(BatchPercolateShardRequest request,
                                             DocumentMapperForType documentMapperForType,
                                             BytesStreamOutput buffer) throws Throwable {
        DocumentMapper docMapper = documentMapperForType.getDocumentMapper();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(buffer.bytes()));

        // Very unclear. Got this condition from mapperService.documentMapperWithAutoCreate:
        // it returns new DocumentMapperForType(mapper, null); in case if mapper was already registered in the internals structures.
        // in ES 1.7 this method has same semantics and the explicit bool value when mapping was modified actually.
        Mapping mapping = documentMapperForType.getMapping();
        if (mapping != null) {
            mappingUpdatedAction.updateMappingOnMasterAsynchronously(
                    request.shardId().getIndex(),
                    request.documentType(),
                    mapping
            );
        }
        return doc;
    }

    private Map<String, BatchPercolateResponseItem> percolateResponses(SearchContext context, Map<String, QueryAndSource> percolateQueries, List<ParsedDocument> parsedDocuments) {
        Map<String, BatchPercolateResponseItem> responses = Maps.newHashMap();
        for(ParsedDocument document : parsedDocuments){
            responses.put(document.id(), new BatchPercolateResponseItem(document.id()));
        }

        for (Map.Entry<String, QueryAndSource> entry : percolateQueries.entrySet()) {
            try{
                executeSearch(context, entry.getValue());
                for (SearchHit searchHit  : context.fetchResult().hits()) {
                    String id = searchHit.getId();
                    BatchPercolateResponseItem batchPercolateResponseItem = responses.get(id);

                    QueryMatch queryMatch = getQueryMatch(entry, searchHit);
                    batchPercolateResponseItem.getMatches().put(queryMatch.getQueryId(), queryMatch);
                }
            }
            catch (Exception e){
                logger.warn(
                        "Failed to execute query. Will not add it to matches. Query ID: {}, Query: {}",
                        e, entry.getKey(), entry.getValue().getQuery());
            }
        }

        return responses;
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

    private QueryMatch getQueryMatch(Map.Entry<String, QueryAndSource> entry, SearchHit searchHit) {
        QueryMatch queryMatch = new QueryMatch();
        queryMatch.setQueryId(entry.getKey());
        queryMatch.setHighlighs(searchHit.highlightFields());
        return queryMatch;
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

}
