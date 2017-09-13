package io.youscan.elasticsearch;

import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class APITests extends AbstractNodesTests {
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

        final String docId = "docId";
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

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

        BytesReference source = new BatchPercolateSourceBuilder().addDoc(
                docBuilder().setDoc(jsonBuilder()
                        .startObject()
                        .field("_id", docId)
                        .field("field1", "the fox is here")
                        .field("field2", "meltwater percolator")
                        .endObject()))
                .toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS).bytes();

        // String body = source.toUtf8();
        String body = "{ \"doc\": { \"field1\": \"the fox is here\", \"field2\": \"meltwater percolator\" } }";

        logger.info("--> Request body:\n" + body);

        Response restResponse = asyncHttpClient.preparePost("http://localhost:9200/"+indexWithPercolator+"/"+docForPercolateType+"/_ypercolate?pretty=true")
                .setHeader("Content-type", "application/json")
                .setBody(body)
                .execute()
                .get();

        String responseBody = restResponse.getResponseBody();

        logger.info("--> Response body:\n" + responseBody);

        assertThat(restResponse.getStatusCode(), equalTo(200));


//        List<String> results = JsonPath.read(responseBody, "$.results");
//        assertThat(results.size(), is(1));
//        String matchedDoc = JsonPath.read(responseBody, "$.results[0].doc");
//        assertThat(matchedDoc, is(docId));
//        List<String> matches = JsonPath.read(responseBody, "$.results[0].matches");
//        assertThat(matches.size(), is(2));
//
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==1)].query_id").get(0), is("1"));
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==1)].highlights.field1[0]").get(0), is("the <b>fox</b> is here"));
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==2)].query_id").get(0), is("2"));
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==2)].highlights.field2[0]").get(0), is("<b>meltwater</b> percolator"));

    }

    @Test
    public void testMultiDocPercolation() throws Throwable{
        final String docId = "docId";
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

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

        BytesReference source = new BatchPercolateSourceBuilder().addDoc(
                docBuilder().setDoc(jsonBuilder()
                        .startObject()
                        .field("_id", docId)
                        .field("field1", "the fox is here")
                        .field("field2", "meltwater percolator")
                        .endObject()))
                .toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS).bytes();

        // String body = source.toUtf8();
        String body = "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"field1\": \"the fox is here\", \"field2\": \"Youscan\" } }\n";

        logger.info("--> Request body:\n" + body);

        Response restResponse = asyncHttpClient.preparePost("http://localhost:9200/"+indexWithPercolator+"/"+docForPercolateType+"/_mypercolate?pretty=true")
            .setHeader("Content-type", "application/json")
            .setBody(body)
            .execute()
            .get();

        String responseBody = restResponse.getResponseBody();

        logger.info("--> Response body:\n" + responseBody);

        assertThat(restResponse.getStatusCode(), equalTo(200));


//        List<String> results = JsonPath.read(responseBody, "$.results");
//        assertThat(results.size(), is(1));
//        String matchedDoc = JsonPath.read(responseBody, "$.results[0].doc");
//        assertThat(matchedDoc, is(docId));
//        List<String> matches = JsonPath.read(responseBody, "$.results[0].matches");
//        assertThat(matches.size(), is(2));
//
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==1)].query_id").get(0), is("1"));
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==1)].highlights.field1[0]").get(0), is("the <b>fox</b> is here"));
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==2)].query_id").get(0), is("2"));
//        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==2)].highlights.field2[0]").get(0), is("<b>meltwater</b> percolator"));

    }
}
