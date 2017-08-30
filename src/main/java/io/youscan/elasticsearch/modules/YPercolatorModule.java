package io.youscan.elasticsearch.modules;

import io.youscan.elasticsearch.index.YPercolatorService;
import org.elasticsearch.common.inject.AbstractModule;

public class YPercolatorModule extends AbstractModule {

    public static final YPercolatorModule INSTANCE = new YPercolatorModule();

    private YPercolatorModule() { }

    @Override
    protected void configure() {
        bind(YPercolatorService.class).asEagerSingleton();
    }
}
