/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration;

import com.meltwater.elasticsearch.plugin.BatchPercolatorPlugin;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.highlight.HighlightBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
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
            .put("cluster.name", "test-cluster-" + NetworkUtilsLegacy.getLocalAddress().getHostName())
            .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
            .put("node.mode", "local")
            .build();

    private static class NetworkUtilsLegacy {

        private static final InetAddress localAddress;

        static {
            InetAddress localAddressX;
            try {
                localAddressX = InetAddress.getLocalHost();
            } catch (Throwable e) {
                // logger.warn("failed to resolve local host, fallback to loopback", e);
                localAddressX = InetAddress.getLoopbackAddress();
            }
            localAddress = localAddressX;
        }

        public static InetAddress getLocalAddress() {
            return localAddress;
        }
    }

    public void putDefaultSettings(Settings.Builder settings) {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings) {
        defaultSettings = Settings.settingsBuilder().put(defaultSettings).put(settings).build();
    }

    public static Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node startNode(String id, Settings.Builder settings) {
        return startNode(id, settings.build());
    }

    public Node startNode(String id, Settings settings) {
        return buildNode(id, settings).start();
    }

    public static Node buildNode(String id) {
        return buildNode(id, EMPTY_SETTINGS);
    }

    public Node buildNode(String id, Settings.Builder settings) {
        return buildNode(id, settings.build());
    }

    public static Node buildNode(String id, Settings settings) {
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

        Collection<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(BatchPercolatorPlugin.class);

        Node node = new PluginsAwareNode(finalSettings, classpathPlugins);

        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public void closeNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Node node(String id) {
        return nodes.get(id);
    }

    public static Client client(String id) {
        return clients.get(id);
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
        FileSystemUtilsLegacy.deleteRecursively(new File(BASE_DIR));
    }

    private static class FileSystemUtilsLegacy{

        /**
         * Deletes the given files recursively. if <tt>deleteRoots</tt> is set to <code>true</code>
         * the given root files will be deleted as well. Otherwise only their content is deleted.
         */
        public static boolean deleteRecursively(File[] roots, boolean deleteRoots) {

            boolean deleted = true;
            for (File root : roots) {
                deleted &= deleteRecursively(root, deleteRoots);
            }
            return deleted;
        }

        /**
         * Deletes the given files recursively including the given roots.
         */
        public static boolean deleteRecursively(File... roots) {
            return deleteRecursively(roots, true);
        }

        /**
         * Delete the supplied {@link java.io.File} - for directories,
         * recursively delete any nested directories or files as well.
         *
         * @param root       the root <code>File</code> to delete
         * @param deleteRoot whether or not to delete the root itself or just the content of the root.
         * @return <code>true</code> if the <code>File</code> was deleted,
         *         otherwise <code>false</code>
         */
        public static boolean deleteRecursively(File root, boolean deleteRoot) {
            if (root != null) {
                File[] children = root.listFiles();
                if (children != null) {
                    for (File aChildren : children) {
                        deleteRecursively(aChildren, true);
                    }
                }

                if (deleteRoot) {
                    return root.delete();
                } else {
                    return true;
                }
            }
            return false;
        }
    }

    public void rollingRestart(){

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

    public XContentBuilder getSource(QueryBuilder query) throws IOException {
        return getSource(query, new HighlightBuilder());
    }

    public XContentBuilder getSource(QueryBuilder query, HighlightBuilder highlightBuilder) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("query", query);
        highlightBuilder.toXContent(builder, new ToXContent.MapParams(Collections.<String, String>emptyMap()));
        builder.endObject();
        return builder;
    }

    public void waitForYellowStatus(Client client) throws ExecutionException, InterruptedException {
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout(new TimeValue(10, TimeUnit.SECONDS)).execute().get();
    }
}
