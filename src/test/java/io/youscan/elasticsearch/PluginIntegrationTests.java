package io.youscan.elasticsearch;

import io.youscan.elasticsearch.plugin.YPercolatorPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

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
}
