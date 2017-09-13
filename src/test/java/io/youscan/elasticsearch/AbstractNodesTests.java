package io.youscan.elasticsearch;

import com.meltwater.elasticsearch.plugin.BatchPercolatorPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

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

}

