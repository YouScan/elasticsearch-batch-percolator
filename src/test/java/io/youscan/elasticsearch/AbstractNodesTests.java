package io.youscan.elasticsearch;

import com.meltwater.elasticsearch.plugin.BatchPercolatorPlugin;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.highlight.HighlightBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractNodesTests {

    public final static String BASE_DIR = "test-temp";

    // Add -Dorg.slf4j.simpleLogger.defaultLogLevel=debug to the run configuration to enable the verbose logging
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private static Map<String, Node> nodes = newHashMap();

    private static Map<String, Client> clients = newHashMap();

    private static Settings defaultSettings = Settings
            .settingsBuilder()
            .put("path.home", BASE_DIR)
            .put("path.data", BASE_DIR)
            .put("plugin.types", BatchPercolatorPlugin.class.getCanonicalName())
            .put("cluster.name", "test-cluster-" + LegacyNetworkUtils.getLocalAddress().getHostName())
            .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
            .put("node.mode", "local")
            .put("index.max_result_window", "2147483647")
            .build();

    public static Node startNode(String id, Collection<Class<? extends Plugin>> classpathPlugins) {
        return buildNode(id, classpathPlugins).start();
    }

    public static Node buildNode(String id, Collection<Class<? extends Plugin>> classpathPlugins) {
        return buildNode(id, EMPTY_SETTINGS, classpathPlugins);
    }

    public static Node buildNode(String id, Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        // String settingsSource = AbstractNodesTests.class.getName().replace('.', '/') + ".yml";
        Settings finalSettings = Settings.settingsBuilder()
                // .loadFromClasspath(settingsSource)
                // .loadFromPath(Paths.get(settingsSource))
                .put(defaultSettings)
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("index.gateway.type") == null) {
            // default to non gateway
            finalSettings = Settings.settingsBuilder().put(finalSettings).put("index.gateway.type", "none").build();
        }
        if (finalSettings.get("cluster.routing.schedule") != null) {
            // decrease the routing schedule so new nodes will be added quickly
            finalSettings = Settings.settingsBuilder().put(finalSettings).put("cluster.routing.schedule", "50ms").build();
        }

        Node node = new ClasspathPluginsAwareNode(finalSettings, classpathPlugins);

        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public static void closeAllNodesAndClear() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
        //FileSystemUtils.deleteRecursively(new File(BASE_DIR));
        LegacyFileSystemUtils.deleteRecursively(new File(BASE_DIR));
    }

    public static Client client(String id) {
        return clients.get(id);
    }

    public XContentBuilder getSource(QueryBuilder query, HighlightBuilder highlightBuilder) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("query", query);
        highlightBuilder.toXContent(builder, new ToXContent.MapParams(Collections.<String, String>emptyMap()));
        builder.endObject();
        return builder;
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(Client client, String... indices) {
        ClusterHealthResponse actionGet = client.admin().cluster()
                .health(Requests.clusterHealthRequest(indices).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client.admin().cluster().prepareState().get().getState().prettyPrint(), client.admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        return actionGet.getStatus();
    }

}

