package io.youscan.elasticsearch;

import io.youscan.elasticsearch.action.YPercolateRequestBuilder;
import io.youscan.elasticsearch.action.YPercolateResponse;
import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class RecoveryTests extends AbstractNodesTests {
    @AfterClass
    public static void closeNodes() {
        closeAllNodesAndClear();
    }

    @Test
    @Ignore("This issue exists in official percolator as well. Fixed in elasticsearch 2.0. " +
            "When running on 2.0 it fails on recovery with UnsupportedOperationException[Query QueryWrapperFilter(_type:~ypercolator) does not implement createWeight]; ]\n" +
            "[elasticsearch[node1][generic][T#3]] WARN org.elasticsearch.indices.cluster - [node1] [[test][1]] marking and sending shard failed due to [failed recovery]")
    public void testRestartNode() throws IOException, ExecutionException, InterruptedException {

        ArrayList<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(YPercolatorPlugin.class);

        Settings extraSettings = Settings.settingsBuilder()
                .put("index.gateway.type", "local").build();

        logger.info("--> Starting one nodes");
        buildNode("node1", extraSettings, classpathPlugins).start();
        Client client = client("node1");

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();

        logger.info("--> Register query");
        client.prepareIndex("test", YPercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder()
                                .startObject()
                                .field("query", matchQuery("field1", "b"))
                                .field("id", 1)
                                .field("group", "g1")
                                .field("query_hash", "hash1")
                                .endObject()
                ).setRefresh(true).execute().actionGet();
        logger.info("--> Restarting node");
        closeNode("node1");
        buildNode("node1", extraSettings, classpathPlugins).start();
        client = client("node1");
        logger.info("Waiting for cluster health to be yellow");
        waitForYellowStatus(client);

        logger.info("--> Percolate doc with field1=b");
        YPercolateResponse response = new YPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new PercolateSourceBuilder().setDoc(new PercolateSourceBuilder.DocBuilder().setDoc(
                        jsonBuilder()
                                .startObject()
                                .field("field1", "b")
                                .endObject())
                        )
                )
                .execute().actionGet();

        assertThat(response.getResults().get(0).getMatches().size(), is(1));

        logger.info("--> Restarting node again (This will trigger another code-path since translog is flushed)");
        closeNode("node1");
        buildNode("node1", extraSettings, classpathPlugins).start();
        client = client("node1");
        logger.info("Waiting for cluster health to be yellow");
        waitForYellowStatus(client);

        logger.info("--> Percolate doc with field1=b");
        response = new YPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new PercolateSourceBuilder().setDoc(new PercolateSourceBuilder.DocBuilder().setDoc(
                        jsonBuilder()
                                .startObject()
                                .field("field1", "b")
                                .endObject()))
                )
                .execute().actionGet();

        assertThat(response.getResults().get(0).getMatches().size(), is(1));
    }
}
