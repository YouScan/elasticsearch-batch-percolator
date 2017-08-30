package io.youscan.elasticsearch.modules;

import com.meltwater.elasticsearch.shard.BatchPercolatorQueriesRegistry;
import org.elasticsearch.common.inject.AbstractModule;

public class YPercolatorShardModule extends AbstractModule {

    public static final YPercolatorShardModule INSTANCE = new YPercolatorShardModule();

    private YPercolatorShardModule() { }

    @Override
    protected void configure() {
        bind(BatchPercolatorQueriesRegistry.class).asEagerSingleton();
    }
}
