package com.redis.riot.core;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisClientOptions {

    private RedisSslOptions sslOptions = new RedisSslOptions();

    private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;

    private ProtocolVersion protocolVersion;

    public RedisSslOptions getSslOptions() {
        return sslOptions;
    }

    public void setSslOptions(RedisSslOptions sslOptions) {
        this.sslOptions = sslOptions;
    }

    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public ClientOptions clientOptions() {
        ClientOptions.Builder builder = clientOptions(ClientOptions.builder());
        return builder.build();
    }

    public ClusterClientOptions clusterClientOptions() {
        ClusterClientOptions.Builder builder = clientOptions(ClusterClientOptions.builder());
        return builder.build();
    }

    private <B extends ClientOptions.Builder> B clientOptions(B builder) {
        builder.autoReconnect(autoReconnect);
        builder.sslOptions(sslOptions.sslOptions());
        builder.protocolVersion(protocolVersion);
        return builder;
    }

}
