package com.redislabs.riot;

import com.redislabs.mesclun.RedisModulesClient;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine.Option;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisOptions {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_DATABASE = 0;
    public static final int DEFAULT_TIMEOUT = 60;
    public static final int DEFAULT_POOL_MAX_TOTAL = 8;

    @Builder.Default
    @Option(names = {"-h", "--hostname"}, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
    private String host = DEFAULT_HOST;
    @Builder.Default
    @Option(names = {"-p", "--port"}, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
    private int port = DEFAULT_PORT;
    @Option(names = {"-s", "--socket"}, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
    private String socket;
    @Option(names = "--user", description = "Used to send ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
    private String username;
    @Option(names = {"-a", "--pass"}, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
    private char[] password;
    @Option(names = {"-u", "--uri"}, arity = "1..*", description = "Server URI.", paramLabel = "<uri>")
    private RedisURI[] uris;
    @Builder.Default
    @Option(names = "--timeout", description = "Redis command timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
    private long timeout = DEFAULT_TIMEOUT;
    @Builder.Default
    @Option(names = {"-n", "--db"}, description = "Database number (default: ${DEFAULT-VALUE}).", paramLabel = "<db>")
    private int database = DEFAULT_DATABASE;
    @Option(names = {"-c", "--cluster"}, description = "Enable cluster mode.")
    private boolean cluster;
    @Option(names = "--tls", description = "Establish a secure TLS connection.")
    private boolean tls;
    @Builder.Default
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
    @Option(names = "--cert", description = "Path to X.509 trusted certificates file in PEM format.", paramLabel = "<file>", hidden = true)
    private File cert;
    @Option(names = "--latency", description = "Show latency metrics.")
    private boolean showMetrics;
    @Builder.Default
    @Option(names = "--pool-max", description = "Max pool connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int poolMaxTotal = DEFAULT_POOL_MAX_TOTAL;
    @Builder.Default
    @Option(names = "--no-auto-reconnect", description = "Auto reconnect on connection loss. True by default.", negatable = true, hidden = true)
    private boolean autoReconnect = true;
    @Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
    private String clientName;

    public static BaseRedisCommands<String, String> commands(AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            return ((RedisClusterClient) client).connect().sync();
        }
        return ((RedisModulesClient) client).connect().sync();
    }

    public List<RedisURI> uris() {
        List<RedisURI> redisURIs = new ArrayList<>();
        if (ObjectUtils.isEmpty(uris)) {
            RedisURI uri = RedisURI.create(host, port);
            uri.setSocket(socket);
            uri.setSsl(tls);
            redisURIs.add(uri);
        } else {
            redisURIs.addAll(Arrays.asList(this.uris));
        }
        for (RedisURI uri : redisURIs) {
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
        return redisURIs;
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
        if (cert != null) {
            builder.trustManager(cert);
        }
        return builder.build();
    }

    public RedisClusterClient redisClusterClient() {
        RedisClusterClient client = RedisClusterClient.create(clientResources(), uris());
        client.setOptions(ClusterClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
        return client;
    }

    public RedisModulesClient redisClient() {
        RedisModulesClient client = RedisModulesClient.create(clientResources(), uris().get(0));
        client.setOptions(ClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
        return client;
    }

    public <T> GenericObjectPoolConfig<T> poolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolMaxTotal);
        return config;
    }

    public RedisModulesClient rediSearchClient() {
        log.info("Creating RediSearch client: {}", this);
        RedisModulesClient client = RedisModulesClient.create(clientResources(), uris().get(0));
        client.setOptions(ClientOptions.builder().autoReconnect(autoReconnect).sslOptions(sslOptions()).build());
        return client;
    }

    public AbstractRedisClient client() {
        if (cluster) {
            log.info("Creating Redis cluster client: {}", this);
            return redisClusterClient();
        }
        log.info("Creating Redis client: {}", this);
        return redisClient();
    }

    public static StatefulConnection<String, String> connection(AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            return ((RedisClusterClient) client).connect();
        }
        return ((RedisModulesClient) client).connect();
    }

}
