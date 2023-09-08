package com.redis.riot.core;

public interface StepConfigurationStrategy {

    void configure(StepBuilder<?, ?> step);

}
