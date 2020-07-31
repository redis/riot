package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine;

import java.io.File;
import java.time.Duration;

public class RedisConnectionOptions {

    @CommandLine.Option(names = {"-h", "--host"}, description = "Server hostname (default: ${DEFAULT-VALUE})", paramLabel = "<host>")
    private String host = "127.0.0.1";
    @CommandLine.Option(names = {"-p", "--port"}, description = "Server port (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int port = 6379;
    @CommandLine.Option(names = {"-s", "--socket"}, description = "Server socket (overrides hostname and port)", paramLabel = "<socket>")
    private String socket;
    @CommandLine.Option(names = {"-a", "--pass"}, arity = "0..1", interactive = true, description = "Password to use when connecting to the server", paramLabel = "<pwd>")
    private String password;
    @CommandLine.Option(names = {"-u", "--uri"}, description = "Server URI", paramLabel = "<uri>")
    private RedisURI redisURI;
    @CommandLine.Option(names = {"-n", "--db"}, description = "Database number", paramLabel = "<int>")
    private Integer database;
    @Getter
    @CommandLine.Option(names = {"-c", "--cluster"}, description = "Enable cluster mode")
    private boolean cluster;
    @CommandLine.Option(names = {"-t", "--tls"}, description = "Establish a secure TLS connection")
    private boolean tls;
    @CommandLine.Option(names = "--ks", description = "Path to keystore", paramLabel = "<file>", hidden = true)
    private File keystore;
    @CommandLine.Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>", hidden = true)
    private String keystorePassword;
    @CommandLine.Option(names = "--ts", description = "Path to truststore", paramLabel = "<file>", hidden = true)
    private File truststore;
    @CommandLine.Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>", hidden = true)
    private String truststorePassword;
    @CommandLine.Option(names = {"-l", "--latency"}, description = "Show latency metrics")
    private boolean showMetrics;
    @Getter
    @CommandLine.Option(names = {"-m", "--pool"}, description = "Max pool connections (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int poolMaxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
    @CommandLine.Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect", hidden = true)
    private boolean noAutoReconnect;

    public RedisURI getRedisURI() {
        if (redisURI == null) {
            return redisURI();
        }
        return redisURI;
    }

    private RedisURI redisURI() {
        RedisURI redisURI = new RedisURI();
        redisURI.setHost(host);
        redisURI.setPort(port);
        redisURI.setSocket(socket);
        if (password!=null) {
            redisURI.setPassword(password);
        }
        if (database!=null) {
            redisURI.setDatabase(database);
        }
        redisURI.setSsl(tls);
        return redisURI;
    }


    public ClientResources getClientResources() {
        if (showMetrics) {
            DefaultClientResources.Builder clientResourcesBuilder = DefaultClientResources.builder();
            clientResourcesBuilder.commandLatencyCollectorOptions(DefaultCommandLatencyCollectorOptions.builder().enable().build());
            clientResourcesBuilder.commandLatencyPublisherOptions(DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
            ClientResources resources = clientResourcesBuilder.build();
            resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent).cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
            return clientResourcesBuilder.build();
        }
        return null;
    }

    public ClusterClientOptions getClientOptions() {
        SslOptions.Builder sslOptionsBuilder = SslOptions.builder();
        if (keystore != null) {
            if (keystorePassword == null) {
                sslOptionsBuilder.keystore(keystore);
            } else {
                sslOptionsBuilder.keystore(keystore, keystorePassword.toCharArray());
            }
        }
        if (truststore != null) {
            if (truststorePassword == null) {
                sslOptionsBuilder.truststore(truststore);
            } else {
                sslOptionsBuilder.truststore(truststore, truststorePassword);
            }
        }
        return ClusterClientOptions.builder().autoReconnect(!noAutoReconnect).sslOptions(sslOptionsBuilder.build()).build();
    }

}
