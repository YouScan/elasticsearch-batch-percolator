package io.youscan.elasticsearch;

import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class PluginIntegrationTests extends AbstractNodesTests {

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
    public void testNodeStartsWithPlugin(){

        Assert.assertTrue("Node started in the setup procedure", true);
    }

    @Test
    public void testQueriesRegistration() throws Throwable {
        logger.info("--> Add dummy doc to auto create the index");
        client.admin().indices().prepareDelete("_all").execute().actionGet();

        final String index = "index1";
        final String type = "type1";

        client.prepareIndex(index, type, "1")
                .setSource("field1", "value1")
                .execute().actionGet();

        logger.info("--> register the queries");
        client.prepareIndex(index, YPercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("query", matchQuery("field1", "b"))
                        .field("group", "g1")
                        .field("query_hash", "hash1")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex(index, YPercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchQuery("field1", "c"))
                        .field("group", "g2")
                        .field("query_hash", "hash2")
                        .endObject()
                ).execute().actionGet();
        client.prepareIndex(index, YPercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject()
                        .field("query", boolQuery()
                                .must(matchQuery("field1", "b"))
                                .must(matchQuery("field1", "c")))
                        .field("group", "g3")
                        .field("query_hash", "hash3").endObject()
                ).execute().actionGet();
        client.prepareIndex(index, YPercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject()
                        .field("query", matchAllQuery())
                        .field("group", "g4")
                        .field("query_hash", "hash4").endObject()
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(index).execute().actionGet();

    }
}
