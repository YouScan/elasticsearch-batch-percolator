package io.youscan.elasticsearch;

import io.youscan.elasticsearch.action.YPercolateRequestBuilder;
import io.youscan.elasticsearch.action.YPercolateResponse;
import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author pelle.berglund
 */
public class ClusterTests extends AbstractNodesTests {

    @AfterClass
    public static void closeNodes() {
        closeAllNodesAndClear();
    }

    @Test
    public void usingRemoteTransport() throws IOException {

        ArrayList<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(YPercolatorPlugin.class);

        //Make sure there is only one shard
        Settings extraSettings = Settings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

        logger.info("--> start first node!");
        buildNode("node1", extraSettings, classpathPlugins).start();
        Client client = client("node1");

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex("test", "type", "1")
                .setSource("field1", "value").execute().actionGet();

        logger.info("--> register a query with highlights");
        client.prepareIndex("test", YPercolatorService.TYPE_NAME, "1")
                .setSource(getSource(termQuery("field1", "fox"), new HighlightBuilder().field("field1").preTags("<b>").postTags("</b>")))
                .execute().actionGet();

        //Start a second node
        logger.info("--> start second node");
        buildNode("node2", extraSettings, classpathPlugins).start();
        client = client("node2");

        //Do percolation request - now the response must be serialized/deserialized
        YPercolateResponse response = new YPercolateRequestBuilder(client).setIndices("test").setDocumentType("type")
                .setSource(new PercolateSourceBuilder().setDoc(
                        new PercolateSourceBuilder.DocBuilder().setDoc(
                            jsonBuilder()
                                    .startObject()
                                    .field("field1", "the fox is here")
                                    .endObject())))
                .execute().actionGet();

        assertThat(response.getShardFailures().length, is(0));
        assertThat(response.getResults().get(0).getMatches().size(), is(1));
    }
}
