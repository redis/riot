package com.redis.riot.cli;

import java.io.File;
import java.time.Duration;

import com.redis.riot.core.RedisClientOptions;

import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class RedisArgs {

    @Option(names = { "-u", "--uri" }, description = "Redis server URI.", paramLabel = "<uri>")
    String uri;

    @Option(names = { "-h", "--host" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
    String host = RedisClientOptions.DEFAULT_HOST;

    @Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
    int port = RedisClientOptions.DEFAULT_PORT;

    @Option(names = { "-s", "--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
    String socket;

    @Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
    String username;

    @Option(names = { "-a",
            "--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
    char[] password;

    @Option(names = "--timeout", description = "Redis command timeout in seconds.", paramLabel = "<sec>")
    long timeout;

    @Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
    int database;

    @Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
    String clientName;

    @Option(names = "--tls", description = "Establish a secure TLS connection.")
    boolean tls;

    @Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
    boolean insecure;

    @Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
    boolean cluster;

    @Option(names = "--metrics-step", description = "Metrics publish interval in seconds. Use 0 to disable metrics publishing. (default: 0).", paramLabel = "<secs>", hidden = true)
    long metricsStep;

    @Option(names = "--no-auto-reconnect", description = "Disable auto-reconnect on connection loss.")
    boolean noAutoReconnect;

    @Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
    ProtocolVersion protocolVersion;

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

    public RedisClientOptions redisOptions() {
        RedisClientOptions options = new RedisClientOptions();
        options.setAutoReconnect(!noAutoReconnect);
        options.setCluster(cluster);
        options.setKey(key);
        options.setKeyCert(keyCert);
        options.setKeyPassword(keyPassword);
        options.setKeystore(keystore);
        options.setKeystorePassword(keystorePassword);
        if (metricsStep > 0) {
            options.setMetricsStep(Duration.ofSeconds(metricsStep));
        }
        options.setProtocolVersion(protocolVersion);
        options.setTrustedCerts(trustedCerts);
        options.setTruststore(truststore);
        options.setTruststorePassword(truststorePassword);
        options.setClientName(clientName);
        options.setDatabase(database);
        options.setHost(host);
        options.setPassword(password);
        options.setPort(port);
        options.setSocket(socket);
        if (timeout > 0) {
            options.setTimeout(Duration.ofSeconds(timeout));
        }
        options.setTls(tls);
        options.setUri(uri);
        options.setUsername(username);
        if (insecure) {
            options.setVerifyPeer(SslVerifyMode.NONE);
        }
        return options;
    }

}
