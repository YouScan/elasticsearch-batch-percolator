package io.youscan.elasticsearch;

import io.youscan.elasticsearch.action.*;
import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import io.youscan.elasticsearch.shard.QueryMatch;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                    .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("query", matchQuery("field1", "c"))
                    .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("query", boolQuery()
                            .must(matchQuery("field1", "b"))
                            .must(matchQuery("field1", "c")))
                    .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("query", matchAllQuery())
                    .endObject()
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
                                .field("field1", "b")
                            .endObject()))
                )
                .execute().actionGet();

        assertThat(response.getResults().size(), is(1));
        assertThat(response.getResults().get(0).getMatches().size(), is(2));

        ArrayList<String> keys = new ArrayList<>();
        for (YPercolateResponseItem item: response.getResults()){
            for (Map.Entry<String, QueryMatch> match: item.getMatches().entrySet()){
                keys.add(match.getKey());
                assertThat(match.getValue().getHighlights().size(), is(0));
            }
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
                    .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("query", matchQuery("field1", "c"))
                    .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("query", boolQuery()
                            .must(matchQuery("field1", "b"))
                            .must(matchQuery("field1", "c"))
                        )
                    .endObject()
                ).execute().actionGet();
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject()
                    .field("query", matchAllQuery())
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        MultiYPercolateResponse responses = new MultiYPercolateRequestBuilder(client)
                .add(new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "b")
                                                .endObject())
                                )
                        )
                )
                .execute().actionGet();

        assertThat(responses.getItems().length, is(1));

        List<YPercolateResponseItem> results = responses.getItems()[0].getResponse().getResults();
        assertThat(results.size(), is(1));
        assertThat(results.get(0).getMatches().size(), is(2));

        ArrayList<String> keys = new ArrayList<>();
        for (YPercolateResponseItem item: results){
            for (Map.Entry<String, QueryMatch> match: item.getMatches().entrySet()){
                keys.add(match.getKey());
                assertThat(match.getValue().getHighlights().size(), is(0));
            }
        }

        assertThat(keys, hasItems("1", "4"));
    }

    @Test
    public void testMultiPercolatioWhenNoMatches() throws Throwable{
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
                    .endObject()
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        MultiYPercolateResponse responses = new MultiYPercolateRequestBuilder(client)
                .add(
                        new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "abalabagama #1")
                                                .endObject())
                                )
                        )
                )
                .add(
                        new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "abalabagama #2")
                                                .endObject())
                                )
                        )
                )
                .add(
                        new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "abalabagama #3")
                                                .endObject())
                                )
                        )
                )
                .execute().actionGet();

        assertThat(responses.getItems().length, is(3));

        assertThat(responses.getItems()[0].getResponse().getResults().size(), is(0));
        assertThat(responses.getItems()[1].getResponse().getResults().size(), is(0));
        assertThat(responses.getItems()[2].getResponse().getResults().size(), is(0));
    }

    @Test
    public void testMultiPercolatioWhenNoQueriesRegistered() throws Throwable{
        logger.info("--> Add dummy doc to auto create the indexWithPercolator");
        client.admin().indices().prepareDelete("_all").execute().actionGet();

        final String indexWithPercolator = "index1";
        final String docForPercolateType = "type1";

        client.prepareIndex(indexWithPercolator, docForPercolateType, "1")
                .setSource("field1", "value1")
                .execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        MultiYPercolateResponse responses = new MultiYPercolateRequestBuilder(client)
                .add(
                        new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "abalabagama #1")
                                                .endObject())
                                )
                        )
                )
                .add(
                        new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "abalabagama #2")
                                                .endObject())
                                )
                        )
                )
                .add(
                        new YPercolateRequestBuilder(client)
                        .setIndices(indexWithPercolator)
                        .setDocumentType(docForPercolateType)
                        .setSource(new PercolateSourceBuilder()
                                .setDoc(docBuilder()
                                        .setDoc(jsonBuilder()
                                                .startObject()
                                                    .field("field1", "abalabagama #3")
                                                .endObject())
                                )
                        )
                )
                .execute().actionGet();

        assertThat(responses.getItems().length, is(3));

        assertThat(responses.getItems()[0].getResponse().getResults().size(), is(0));
        assertThat(responses.getItems()[1].getResponse().getResults().size(), is(0));
        assertThat(responses.getItems()[2].getResponse().getResults().size(), is(0));
    }
}
