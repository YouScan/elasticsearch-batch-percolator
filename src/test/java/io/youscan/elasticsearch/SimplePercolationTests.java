package io.youscan.elasticsearch;

import io.youscan.elasticsearch.action.MultiYPercolateRequestBuilder;
import io.youscan.elasticsearch.action.MultiYPercolateResponse;
import io.youscan.elasticsearch.action.YPercolateRequestBuilder;
import io.youscan.elasticsearch.action.YPercolateResponse;
import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimplePercolationTests extends AbstractNodesTests {

    static Client client;

    @BeforeClass
    public static void createNodes() throws Exception {
        ArrayList<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(YPercolatorPlugin.class);
        startNode("node1", classpathPlugins);
        client = client("node1");
    }

    @AfterClass
    public static void closeNodes() {
        closeAllNodesAndClear();
    }

    @Test
    public void testSingleDocPercolation() throws Throwable {
        logger.info("--> Add dummy doc to auto create the indexWithPercolator");
        client.admin().indices().prepareDelete("_all").execute().actionGet();

        final String indexWithPercolator = "index1";
        final String docForPercolateType = "type1";

        client.prepareIndex(indexWithPercolator, docForPercolateType, "1")
                .setSource("field1", "value1")
                .execute().actionGet();

        logger.info("--> register the queries");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("query", matchQuery("field1", "b"))
                        .field("group", "g1")
                        .field("query_hash", "hash1")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchQuery("field1", "c"))
                        .field("group", "g2")
                        .field("query_hash", "hash2")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject()
                        .field("query", boolQuery()
                                .must(matchQuery("field1", "b"))
                                .must(matchQuery("field1", "c")))
                        .field("group", "g3")
                        .field("query_hash", "hash3").endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchAllQuery())
                        .field("group", "g4")
                        .field("query_hash", "hash4").endObject()
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        YPercolateResponse response = new YPercolateRequestBuilder(client)
                .setIndices(indexWithPercolator)
                .setDocumentType(docForPercolateType)
                .setSource(new PercolateSourceBuilder()
                    .setDoc(docBuilder()
                            .setDoc(jsonBuilder()
                                    .startObject()
                                        .field("_id", "1")
                                        .field("field1", "b")
                                    .endObject()))
                )
                .execute().actionGet();

        assertThat(response.getMatches().length, is(2));

        ArrayList<String> keys = new ArrayList<>();
        for (YPercolateResponse.Match item: response.getMatches()){
            keys.add(item.getId().string());

            assertThat(item.getHighlightFields().size(), is(0));
        }

        assertThat(keys, hasItems("1", "4"));
    }

    @Test
    public void testMultiPercolation() throws Throwable{
        logger.info("--> Add dummy doc to auto create the indexWithPercolator");
        client.admin().indices().prepareDelete("_all").execute().actionGet();

        final String indexWithPercolator = "index1";
        final String docForPercolateType = "type1";

        client.prepareIndex(indexWithPercolator, docForPercolateType, "1")
                .setSource("field1", "value1")
                .execute().actionGet();

        logger.info("--> register the queries");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("query", matchQuery("field1", "b"))
                        .field("group", "g1")
                        .field("query_hash", "hash1")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchQuery("field1", "c"))
                        .field("group", "g2")
                        .field("query_hash", "hash2")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject()
                        .field("query", boolQuery()
                                .must(matchQuery("field1", "b"))
                                .must(matchQuery("field1", "c")))
                        .field("group", "g3")
                        .field("query_hash", "hash3").endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchAllQuery())
                        .field("group", "g4")
                        .field("query_hash", "hash4").endObject()
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        MultiYPercolateResponse response = new MultiYPercolateRequestBuilder(client)
                .add(new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                .field("_id", "1")
                                                .field("field1", "b")
                                                .endObject()))
                        )
                )
                .execute().actionGet();

        assertThat(response.getItems().length, is(1));

//        assertThat(response.getMatches().length, is(2));
//
//        ArrayList<String> keys = new ArrayList<>();
//        for (YPercolateResponse.Match item: response.getMatches()){
//            keys.add(item.getId().string());
//
//            assertThat(item.getHighlightFields().size(), is(0));
//        }
//
//        assertThat(keys, hasItems("1", "4"));
    }
}
