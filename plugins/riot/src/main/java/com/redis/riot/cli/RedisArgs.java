package com.redis.riot.cli;

import java.io.File;
import java.time.Duration;

import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RedisOptions;
import com.redis.riot.core.RedisSslOptions;

import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisArgs {

    @ArgGroup(exclusive = false)
    RedisUriArgs uriArgs = new RedisUriArgs();

    @ArgGroup(exclusive = false)
    ClientArgs clientArgs = new ClientArgs();

    @Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
    boolean cluster;

    @Option(names = "--metrics-step", description = "Metrics publish interval in seconds. Use 0 to disable metrics publishing. (default: 0).", paramLabel = "<secs>", hidden = true)
    long metricsStep;

    public static class ClientArgs {

        @Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect on connection loss.")
        boolean noAutoReconnect;

        @Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
        ProtocolVersion protocolVersion;

        @ArgGroup(exclusive = false)
        SslArgs sslArgs = new SslArgs();

        public RedisClientOptions clientOptions() {
            RedisClientOptions options = new RedisClientOptions();
            options.setAutoReconnect(!noAutoReconnect);
            options.setSslOptions(sslArgs.sslOptions());
            options.setProtocolVersion(protocolVersion);
            return options;
        }

    }

    public static class SslArgs {

        @Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
        File keystore;

        @Option(names = "--ks-pwd", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>", hidden = true)
        char[] keystorePassword;

        @Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
        File truststore;

        @Option(names = "--ts-pwd", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>", hidden = true)
        char[] truststorePassword;

        @Option(names = "--cert", description = "X.509 cert chain file to authenticate (PEM).", paramLabel = "<file>")
        File keyCert;

        @Option(names = "--key", description = "PKCS#8 private key file to authenticate (PEM).", paramLabel = "<file>")
        File key;

        @Option(names = "--key-pwd", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "<pwd>")
        char[] keyPassword;

        @Option(names = "--cacert", description = "X.509 CA certificate file to verify with.", paramLabel = "<file>")
        File trustedCerts;

        public RedisSslOptions sslOptions() {
            RedisSslOptions options = new RedisSslOptions();
            options.setKey(key);
            options.setKeyCert(keyCert);
            options.setKeyPassword(keyPassword);
            options.setKeystore(keystore);
            options.setKeystorePassword(keystorePassword);
            options.setTrustedCerts(trustedCerts);
            options.setTruststore(truststore);
            options.setTruststorePassword(truststorePassword);
            return options;
        }

    }

    public RedisOptions redisClientOptions() {
        RedisOptions options = new RedisOptions();
        options.setClientOptions(clientArgs.clientOptions());
        options.setCluster(cluster);
        if (metricsStep > 0) {
            options.setMetricsStep(Duration.ofSeconds(metricsStep));
        }
        options.setUriOptions(uriArgs.redisUriOptions());
        return options;
    }

}
