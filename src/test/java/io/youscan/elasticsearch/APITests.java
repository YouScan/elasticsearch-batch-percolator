package io.youscan.elasticsearch;

import com.jayway.jsonpath.JsonPath;
import com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder;
import com.meltwater.elasticsearch.index.BatchPercolatorService;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.youscan.elasticsearch.index.YPercolatorService;
import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import net.minidev.json.JSONArray;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.meltwater.elasticsearch.action.BatchPercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;


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

        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

        final String indexWithPercolator = "index1";
        final String docForPercolateType = "type1";

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex(indexWithPercolator, docForPercolateType, "1")
                .setSource("field1", "value", "field2", "value").execute().actionGet();

        logger.info("--> register query1 with highlights");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "001")
                .setSource(getSource(
                        termQuery("field1", "fox"),
                        new HighlightBuilder()
                                .field("field1")
                                .preTags("<b>")
                                .postTags("</b>"))
                )
                .execute().actionGet();

        logger.info("--> register query2 with highlights");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "002")
                .setSource(getSource(
                        termQuery("field2", "youscan"),
                        new HighlightBuilder()
                                .requireFieldMatch(true)
                                .order("score")
                                .highlightQuery(termQuery("field2", "youscan"))
                                .field("field2")
                                .preTags("<b>")
                                .postTags("</b>")))
                .execute().actionGet();

        logger.info("--> register query3 to have at least one match");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "003")
                .setSource(jsonBuilder()
                    .startObject()
                        .field("query", matchAllQuery())
                    .endObject()
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Doing percolation with Rest API");

        String body = jsonBuilder()
                .startObject()
                    .rawField("doc", jsonBuilder()
                            .startObject()
                                .field("docSlot", "0")
                                .field("field1", "the fox is here")
                                .field("field2", "youscan percolator")
                            .endObject().bytes())
                .endObject()
                //.prettyPrint()
                .string();

        logger.info("   --> Request body:\n" + body);

        Response restResponse = asyncHttpClient.preparePost("http://localhost:9200/"+indexWithPercolator+"/"+docForPercolateType+"/_ypercolate?pretty=true")
                .setHeader("content-type", "application/json")
                .setBody(body)
                .execute()
                .get();

        String responseBody = restResponse.getResponseBody();

        logger.info("   --> Response body:\n" + responseBody);

        assertThat(restResponse.getStatusCode(), equalTo(200));

        List<String> results = JsonPath.read(responseBody, "$.results");
        assertThat(results.size(), is(1));
        String matchedDoc = JsonPath.read(responseBody, "$.results[0].doc");
        assertThat(matchedDoc, is("0"));
        List<String> matches = JsonPath.read(responseBody, "$.results[0].matches");
        assertThat(matches.size(), is(3));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==\"001\")].query_id").get(0), is("001"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==\"001\")].highlights.field1[0]").get(0), is("the <b>fox</b> is here"));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==\"002\")].query_id").get(0), is("002"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==\"002\")].highlights.field2[0]").get(0), is("<b>youscan</b> percolator"));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.results[0].matches[?(@.query_id==\"003\")].query_id").get(0), is("003"));
        assertThat(JsonPath.<JSONArray>read(responseBody, "$.results[0].matches[?(@.query_id==\"003\")].highlights[0]").size(), is(0));
    }

    @Test
    public void testMultiDocPercolation() throws Throwable{
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

        final String indexWithPercolator = "index1";
        final String docForPercolateType = "type1";

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex(indexWithPercolator, docForPercolateType, "1")
                .setSource("field1", "value", "field2", "value").execute().actionGet();

        logger.info("--> register query1 with highlights");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "001")
                .setSource(getSource(
                        termQuery("field1", "fox"),
                        new HighlightBuilder()
                                .field("field1")
                                .preTags("<b>")
                                .postTags("</b>"))
                )
                .execute().actionGet();

        logger.info("--> register query2 with highlights");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "002")
                .setSource(getSource(
                        termQuery("field2", "youscan"),
                        new HighlightBuilder()
                                .requireFieldMatch(true)
                                .order("score")
                                .highlightQuery(termQuery("field2", "youscan"))
                                .field("field2")
                                .preTags("<b>")
                                .postTags("</b>")))
                .execute().actionGet();

        logger.info("--> register query3 to have at least one match");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "003")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("query", matchAllQuery())
                        .endObject()
                ).execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Doing percolation with Rest API");

        String body =
                "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"docSlot\": \"0\", \"field1\": \"the fox is here\" } }\n" +

                "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"docSlot\": \"1\", \"field2\": \"youscan percolator\" } }\n" +

                "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"docSlot\": \"2\", \"field1\": \"bad wolf\", \"field2\": \"dr who\" } }\n"
        ;

        logger.info("--> Request body:\n" + body);

        Response restResponse = asyncHttpClient.preparePost("http://localhost:9200/"+indexWithPercolator+"/"+docForPercolateType+"/_mypercolate?pretty=true")
            .setHeader("Content-type", "application/json")
            .setBody(body)
            .execute()
            .get();

        String responseBody = restResponse.getResponseBody();

        logger.info("--> Response body:\n" + responseBody);

        assertThat(restResponse.getStatusCode(), equalTo(200));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses").size(), is(3));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results").size(), is(1));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results").size(), is(1));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[2].results").size(), is(1));

        assertThat(JsonPath.<String>read(responseBody, "$.responses[0].results[0].doc"), is("0"));
        assertThat(JsonPath.<String>read(responseBody, "$.responses[1].results[0].doc"), is("1"));
        assertThat(JsonPath.<String>read(responseBody, "$.responses[2].results[0].doc"), is("2"));

        assertThat(JsonPath.<String>read(responseBody, "$.responses[0].results[0].doc"), is("0"));
        assertThat(JsonPath.<String>read(responseBody, "$.responses[1].results[0].doc"), is("1"));
        assertThat(JsonPath.<String>read(responseBody, "$.responses[2].results[0].doc"), is("2"));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results[0].matches").size(), is(2));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results[0].matches").size(), is(2));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[2].results[0].matches").size(), is(1));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results[0].matches").size(), is(2));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results[0].matches[?(@.query_id==\"001\")].query_id").get(0), is("001"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results[0].matches[?(@.query_id==\"001\")].highlights.field1[0]").get(0), is("the <b>fox</b> is here"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results[0].matches[?(@.query_id==\"003\")].query_id").get(0), is("003"));
        assertThat(JsonPath.<JSONArray>read(responseBody, "$.responses[0].results[0].matches[?(@.query_id==\"003\")].highlights[0]").size(), is(0));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results[0].matches").size(), is(2));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results[0].matches[?(@.query_id==\"002\")].query_id").get(0), is("002"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results[0].matches[?(@.query_id==\"002\")].highlights.field2[0]").get(0), is("<b>youscan</b> percolator"));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results[0].matches[?(@.query_id==\"003\")].query_id").get(0), is("003"));
        assertThat(JsonPath.<JSONArray>read(responseBody, "$.responses[1].results[0].matches[?(@.query_id==\"003\")].highlights[0]").size(), is(0));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[2].results[0].matches").size(), is(1));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[2].results[0].matches[?(@.query_id==\"003\")].query_id").get(0), is("003"));
        assertThat(JsonPath.<JSONArray>read(responseBody, "$.responses[2].results[0].matches[?(@.query_id==\"003\")].highlights[0]").size(), is(0));
    }

    @Test
    public void testMultiDocPercolationWithNoMatches() throws Throwable{
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();

        final String indexWithPercolator = "index1";
        final String docForPercolateType = "type1";

        logger.info("--> Add dummy doc");
        client.admin().indices().prepareDelete("_all").execute().actionGet();
        client.prepareIndex(indexWithPercolator, docForPercolateType, "1")
                .setSource("field1", "value", "field2", "value").execute().actionGet();

        logger.info("--> register query1 with highlights");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "001")
                .setSource(getSource(
                        termQuery("field1", "fox"),
                        new HighlightBuilder()
                                .field("field1")
                                .preTags("<b>")
                                .postTags("</b>"))
                )
                .execute().actionGet();

        logger.info("--> register query2 with highlights");
        client.prepareIndex(indexWithPercolator, YPercolatorService.TYPE_NAME, "002")
                .setSource(getSource(
                        termQuery("field2", "youscan"),
                        new HighlightBuilder()
                                .requireFieldMatch(true)
                                .order("score")
                                .highlightQuery(termQuery("field2", "youscan"))
                                .field("field2")
                                .preTags("<b>")
                                .postTags("</b>")))
                .execute().actionGet();

        client.admin().indices().prepareRefresh(indexWithPercolator).execute().actionGet();

        logger.info("--> Doing percolation with Rest API");

        String body =
                "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"docSlot\": \"0\", \"field1\": \"the fox is here\" } }\n" +

                "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"docSlot\": \"1\", \"field2\": \"youscan percolator\" } }\n" +

                "{ \"percolate\": { \"index\":\""+ indexWithPercolator +"\", \"type\":\"" + docForPercolateType + "\" } }\n" +
                "{ \"doc\": { \"docSlot\": \"2\", \"field1\": \"bad wolf\", \"field2\": \"dr who\" } }\n"
        ;

        logger.info("--> Request body:\n" + body);

        Response restResponse = asyncHttpClient.preparePost("http://localhost:9200/"+indexWithPercolator+"/"+docForPercolateType+"/_mypercolate?pretty=true")
            .setHeader("Content-type", "application/json")
            .setBody(body)
            .execute()
            .get();

        String responseBody = restResponse.getResponseBody();

        logger.info("--> Response body:\n" + responseBody);

        assertThat(restResponse.getStatusCode(), equalTo(200));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses").size(), is(3));

        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[0].results").size(), is(1));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[1].results").size(), is(1));
        assertThat(JsonPath.<List<String>>read(responseBody, "$.responses[2].results").size(), is(0));

        assertThat(JsonPath.<String>read(responseBody, "$.responses[0].results[0].doc"), is("0"));
        assertThat(JsonPath.<String>read(responseBody, "$.responses[1].results[0].doc"), is("1"));
    }
}
