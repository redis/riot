package com.redislabs.riot.test;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.RedisURI;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;
import redis.embedded.util.OS;

import java.io.InputStream;
import java.nio.charset.Charset;

public abstract class BaseTest {

    private final static String COMMAND_PREAMBLE = "❯";

    private final static int REDIS_PORT = 6379;
    private final static String REDIS_HOST = "localhost";

    private static RedisServer server;
    private static RediSearchClient client;
    protected static StatefulRediSearchConnection<String, String> connection;

    @BeforeAll
    public static void setup() {
        server = serverBuilder(REDIS_PORT).setting("notify-keyspace-events AK").setting("loadmodule /Users/jruaux/git/RediSearch/build/redisearch.so").build();
        server.start();
        client = RediSearchClient.create(RedisURI.create(REDIS_HOST, REDIS_PORT));
        connection = client.connect();
    }

    protected static RedisServerBuilder serverBuilder(int port) {
        RedisExecProvider provider = RedisExecProvider.defaultProvider().override(OS.MAC_OS_X, "/usr/local/bin/redis-server");
        return RedisServer.builder().redisExecProvider(provider).port(port);
    }

    @BeforeEach
    public void flushAll() {
        connection.sync().flushall();
    }

    protected RediSearchCommands<String, String> commands() {
        return connection.sync();
    }

    @AfterAll
    public static void teardown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.stop();
        }
    }

    protected int runFile(String filename, Object... args) {
        try (InputStream inputStream = getClass().getResourceAsStream(filename)) {
            return runCommand(removePreamble(IOUtils.toString(inputStream, Charset.defaultCharset())), args);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    protected int runCommand(String command, Object... args) throws Exception {
        return execute(CommandLineUtils.translateCommandline(String.format(command, args)));
    }

    protected abstract int execute(String[] args) throws Exception;

    protected String commandPrefix() {
        return COMMAND_PREAMBLE + " " + applicationName();
    }

    protected abstract String applicationName();

    private String removePreamble(String command) {
        if (command.startsWith(commandPrefix())) {
            return command.substring(commandPrefix().length());
        }
        return command;
    }



}
