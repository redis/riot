package com.redislabs.riot;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.Data;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine.Option;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
public class RedisOptions {

    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_DATABASE = 0;
    public static final int DEFAULT_TIMEOUT = 60;
    public static final int DEFAULT_POOL_MAX_TOTAL = 8;

    @Option(names = {"-h", "--hostname"}, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
    private String host = DEFAULT_HOST;
    @Option(names = {"-p", "--port"}, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
    private int port = DEFAULT_PORT;
    @Option(names = {"-s", "--socket"}, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
    private String socket;
    @Option(names = "--user", description = "Used to send ACL style 'AUTH username pass'. Needs password.", paramLabel = "<username>")
    private String username;
    @Option(names = {"-a", "--pass"}, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
    private char[] password;
    @Option(names = {"-u", "--uri"}, arity = "0..*", description = "Server URI.", paramLabel = "<uri>")
    private List<RedisURI> uris = new ArrayList<>();
    @Option(names = "--timeout", description = "Redis command timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
    private long timeout = DEFAULT_TIMEOUT;
    @Option(names = {"-n", "--db"}, description = "Database number (default: ${DEFAULT-VALUE}).", paramLabel = "<db>")
    private int database = DEFAULT_DATABASE;
    @Option(names = {"-c", "--cluster"}, description = "Enable cluster mode.")
    private boolean cluster;
    @Option(names = "--tls", description = "Establish a secure TLS connection.")
    private boolean tls;
    @Option(names = "--no-verify-peer", description = "Verify peers when using TLS. True by default.", negatable = true)
    private boolean verifyPeer = true;
    @Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
    private File keystore;
    @Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>", hidden = true)
    private String keystorePassword;
    @Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
    private File truststore;
    @Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>", hidden = true)
    private String truststorePassword;
    @Option(names = "--latency", description = "Show latency metrics.")
    private boolean showMetrics;
    @Option(names = "--pool-max", description = "Max pool connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int poolMaxTotal = DEFAULT_POOL_MAX_TOTAL;
    @Option(names = "--no-auto-reconnect", description = "Auto reconnect on connection loss. True by default.", negatable = true, hidden = true)
    private boolean autoReconnect = true;
    @Option(names = "--client", description = "Client name used to connect to Redis.")
    private String clientName;

    public List<RedisURI> uris() {
        List<RedisURI> uris = new ArrayList<>(this.uris);
        if (uris.isEmpty()) {
            RedisURI uri = new RedisURI();
            uri.setHost(host);
            uri.setPort(port);
            uri.setSocket(socket);
            uri.setSsl(tls);
            uris.add(uri);
        }
        for (RedisURI uri : uris) {
            uri.setVerifyPeer(verifyPeer);
            if (username != null) {
                uri.setUsername(username);
            }
            if (password != null) {
                uri.setPassword(password);
            }
            if (database != uri.getDatabase()) {
                uri.setDatabase(database);
            }
            if (timeout != uri.getTimeout().getSeconds()) {
                uri.setTimeout(Duration.ofSeconds(timeout));
            }
            if (clientName != null) {
                uri.setClientName(clientName);
            }
        }
        return uris;
    }

    private ClientResources clientResources() {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        if (showMetrics) {
            builder.commandLatencyRecorder(CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
            builder.commandLatencyPublisherOptions(DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
            ClientResources resources = builder.build();
            resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent).cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
        }
        return builder.build();
    }

    private SslOptions sslOptions() {
        SslOptions.Builder builder = SslOptions.builder();
        if (keystore != null) {
            if (keystorePassword == null) {
                builder.keystore(keystore);
            } else {
                builder.keystore(keystore, keystorePassword.toCharArray());
            }
        }
        if (truststore != null) {
            if (truststorePassword == null) {
                builder.truststore(truststore);
            } else {
                builder.truststore(truststore, truststorePassword);
            }
        }
        return builder.build();
    }

    public RedisClusterClient redisClusterClient() {
        RedisClusterClient client = RedisClusterClient.create(clientResources(), uris());
        client.setOptions(ClusterClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
        return client;
    }

    public RedisClient redisClient() {
        RedisClient client = RedisClient.create(clientResources(), uris().get(0));
        client.setOptions(ClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
        return client;
    }

    public <T extends StatefulConnection<String, String>> GenericObjectPoolConfig<T> poolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolMaxTotal);
        return config;
    }

}
