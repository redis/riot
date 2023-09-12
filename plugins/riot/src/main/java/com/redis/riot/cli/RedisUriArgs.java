package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.RedisUriOptions;

import io.lettuce.core.SslVerifyMode;
import picocli.CommandLine.Option;

public class RedisUriArgs {

    @Option(names = { "-u", "--uri" }, description = "Redis server URI.", paramLabel = "<uri>")
    String uri;

    @Option(names = { "-h", "--host" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
    String host = RedisUriOptions.DEFAULT_HOST;

    @Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
    int port = RedisUriOptions.DEFAULT_PORT;

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

    public RedisUriOptions redisUriOptions() {
        RedisUriOptions options = new RedisUriOptions();
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
