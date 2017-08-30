package io.youscan.elasticsearch.plugin;

import io.youscan.elasticsearch.action.MultiYPercolateAction;
import io.youscan.elasticsearch.action.TransportMultiYPercolateAction;
import io.youscan.elasticsearch.modules.YPercolatorModule;
import io.youscan.elasticsearch.modules.YPercolatorShardModule;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

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

    /* Invoked on component assembly. */
    public void onModule(ActionModule module) {
        module.registerAction(MultiYPercolateAction.INSTANCE, TransportMultiYPercolateAction.class);
        logger.debug("on ActionModule: Registered MultiYPercolateAction instance");
    }
}
