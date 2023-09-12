package com.redis.riot.core;

import java.time.Duration;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

public class RedisOptions {

    private RedisUriOptions uriOptions = new RedisUriOptions();

    private RedisClientOptions clientOptions = new RedisClientOptions();

    private boolean cluster;

    private Duration metricsStep;

    public RedisUriOptions getUriOptions() {
        return uriOptions;
    }

    public void setUriOptions(RedisUriOptions uriOptions) {
        this.uriOptions = uriOptions;
    }

    public RedisClientOptions getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(RedisClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }

    public boolean isCluster() {
        return cluster;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    public Duration getMetricsStep() {
        return metricsStep;
    }

    public void setMetricsStep(Duration metricsStep) {
        this.metricsStep = metricsStep;
    }

    public AbstractRedisClient client(RedisURI redisURI) {
        ClientResources resources = clientResources();
        if (cluster) {
            RedisModulesClusterClient client = RedisModulesClusterClient.create(resources, redisURI);
            client.setOptions(clientOptions.clusterClientOptions());
            return client;
        }
        RedisModulesClient client = RedisModulesClient.create(resources, redisURI);
        client.setOptions(clientOptions.clientOptions());
        return client;
    }

    public ClientResources clientResources() {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        if (BatchUtils.isPositive(metricsStep)) {
            builder.commandLatencyRecorder(commandLatencyRecorder());
            builder.commandLatencyPublisherOptions(commandLatencyPublisherOptions());
        }
        return builder.build();
    }

    private EventPublisherOptions commandLatencyPublisherOptions() {
        return DefaultEventPublisherOptions.builder().eventEmitInterval(metricsStep).build();
    }

    private CommandLatencyRecorder commandLatencyRecorder() {
        return CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build());
    }

}
