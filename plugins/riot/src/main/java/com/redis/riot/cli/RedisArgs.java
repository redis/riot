package com.redis.riot.cli;

import java.io.File;
import java.time.Duration;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.SslOptions.Resource;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import picocli.CommandLine.Option;

public class RedisArgs {

    public static final String DEFAULT_HOST = "127.0.0.1";

    public static final int DEFAULT_PORT = 6379;

    public static final Duration DEFAULT_METRICS_STEP = Duration.ofSeconds(5);

    @Option(names = { "-u", "--uri" }, description = "Redis server URI.", paramLabel = "<uri>")
    private String uri;

    @Option(names = { "-h", "--hostname" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
    private String host = DEFAULT_HOST;

    @Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
    private int port = DEFAULT_PORT;

    @Option(names = { "-s", "--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
    private String socket;

    @Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
    private String username;

    @Option(names = { "-a",
            "--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
    private char[] password;

    @Option(names = "--timeout", description = "Redis command timeout in seconds.", paramLabel = "<sec>")
    private Long timeout;

    @Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
    private int database;

    @Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
    private boolean cluster;

    @Option(names = "--tls", description = "Establish a secure TLS connection.")
    private boolean tls;

    @Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
    private boolean insecure;

    @Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
    private File keystore;

    @Option(names = "--ks-pwd", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>", hidden = true)
    private char[] keystorePassword;

    @Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
    private File truststore;

    @Option(names = "--ts-pwd", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>", hidden = true)
    private char[] truststorePassword;

    @Option(names = "--cert", description = "X.509 cert chain file to authenticate (PEM).", paramLabel = "<file>")
    private File keyCert;

    @Option(names = "--key", description = "PKCS#8 private key file to authenticate (PEM).", paramLabel = "<file>")
    private File key;

    @Option(names = "--key-pwd", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "<pwd>")
    private char[] keyPassword;

    @Option(names = "--cacert", description = "X.509 CA certificate file to verify with.", paramLabel = "<file>")
    private File trustedCerts;

    @Option(names = "--metrics", description = "Show latency metrics.")
    private boolean showMetrics;

    @Option(names = "--metrics-step", description = "Metrics publish interval in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<secs>", hidden = true)
    private long metricsStep = DEFAULT_METRICS_STEP.toSeconds();

    @Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect on connection loss.")
    private boolean noAutoReconnect;

    @Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
    private String clientName;

    @Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
    private ProtocolVersion protocolVersion;

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    @SuppressWarnings("deprecation")
    public RedisURI uri() {
        RedisURI redisURI = uri == null ? RedisURI.create(host, port) : RedisURI.create(uri);
        if (database > 0) {
            redisURI.setDatabase(database);
        }
        redisURI.setClientName(clientName);
        redisURI.setUsername(username);
        redisURI.setPassword(password);
        redisURI.setSocket(socket);
        redisURI.setSsl(tls);
        redisURI.setVerifyPeer(!insecure);
        if (timeout != null) {
            redisURI.setTimeout(Duration.ofSeconds(timeout));
        }
        return redisURI;
    }

    public AbstractRedisClient client() {
        return client(uri());
    }

    public AbstractRedisClient client(RedisURI redisURI) {
        ClientResources resources = clientResources();
        if (cluster) {
            RedisModulesClusterClient client = RedisModulesClusterClient.create(resources, redisURI);
            ClusterClientOptions.Builder options = ClusterClientOptions.builder();
            configure(options);
            client.setOptions(options.build());
            return client;
        }
        RedisModulesClient client = RedisModulesClient.create(resources, redisURI);
        ClientOptions.Builder options = ClientOptions.builder();
        configure(options);
        client.setOptions(options.build());
        return client;
    }

    public ClientResources clientResources() {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        if (showMetrics) {
            builder.commandLatencyRecorder(
                    CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build()));
            builder.commandLatencyPublisherOptions(
                    DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(metricsStep)).build());

        }
        return builder.build();
    }

    private void configure(ClientOptions.Builder builder) {
        builder.autoReconnect(!noAutoReconnect);
        builder.sslOptions(sslOptions());
        builder.protocolVersion(protocolVersion);
    }

    private SslOptions sslOptions() {
        Builder ssl = SslOptions.builder();
        if (key != null) {
            ssl.keyManager(keyCert, key, keyPassword);
        }
        if (keystore != null) {
            ssl.keystore(keystore, keystorePassword);
        }
        if (truststore != null) {
            ssl.truststore(Resource.from(truststore), truststorePassword);
        }
        if (trustedCerts != null) {
            ssl.trustManager(trustedCerts);
        }
        return ssl.build();
    }

}
