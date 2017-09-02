package io.youscan.elasticsearch.plugin;

import com.meltwater.elasticsearch.rest.RestBatchPercolateAction;
import io.youscan.elasticsearch.action.MultiYPercolateAction;
import io.youscan.elasticsearch.action.TransportMultiYPercolateAction;
import io.youscan.elasticsearch.action.TransportYPercolateAction;
import io.youscan.elasticsearch.action.YPercolateAction;
import io.youscan.elasticsearch.modules.YPercolatorModule;
import io.youscan.elasticsearch.modules.YPercolatorShardModule;
import io.youscan.elasticsearch.rest.RestMultiYPercolateAction;
import io.youscan.elasticsearch.rest.RestYPercolateAction;
import io.youscan.elasticsearch.shard.YPercolatorQueriesRegistry;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;

public class YPercolatorPlugin extends Plugin {

    public static final String NAME = "Y-Percolator";

    private final ESLogger logger;
    private final Settings settings;

    public YPercolatorPlugin(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger("plugin.ypercolator");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch 2.* percolator with two-phase querying and bulk API support. Based on `elasticsearch-batch-percolator` by Meltwater";
    }

    @Override
    public Collection<Module> indexModules(Settings indexSettings) {
        Collection<Module> modules = new ArrayList<>(1);
        modules.add(YPercolatorModule.INSTANCE);
        logger.debug("indexModules: Registered YPercolatorModule.INSTANCE");
        return modules;
    }

    @Override
    public Collection<Module> shardModules(Settings indexSettings) {
        Collection<Module> modules = new ArrayList<>(1);
        modules.add(YPercolatorShardModule.INSTANCE);
        logger.debug("shardModules: Registered YPercolatorShardModule.INSTANCE");
        return modules;
    }

    @Override
    public Collection<Class<? extends Closeable>> shardServices() {
        Collection<Class<? extends Closeable>> shardServices = new ArrayList<>(1);
        shardServices.add(YPercolatorQueriesRegistry.class);
        logger.debug("shardServices: Registered YPercolatorQueriesRegistry.class");
        return shardServices;
    }

    /* Invoked on component assembly. */
    public void onModule(ActionModule module) {
        module.registerAction(YPercolateAction.INSTANCE, TransportYPercolateAction.class);
        logger.debug("ActionModule: Registered YPercolateAction instance");

        module.registerAction(MultiYPercolateAction.INSTANCE, TransportMultiYPercolateAction.class);
        logger.debug("ActionModule: Registered MultiYPercolateAction instance");
    }

    /* Invoked on component assembly. */
    public void onModule(RestModule module) {
        module.addRestAction(RestYPercolateAction.class);
        logger.debug("RestModule: Registered RestYPercolateAction.class");

        module.addRestAction(RestMultiYPercolateAction.class);
        logger.debug("RestModule: Registered RestMultiYPercolateAction.class");
    }
}
