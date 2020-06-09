package com.redislabs.riot;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.Application;
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

public class BaseTest {

    private final static String COMMAND_PREAMBLE = "$ riot ";

    private final static int REDIS_PORT = 6379;
    private final static String REDIS_HOST = "localhost";

    private static RedisServer server;
    private static RediSearchClient client;
    private static StatefulRediSearchConnection<String, String> connection;
    protected static final int BEER_COUNT = 2410;

    @BeforeAll
    public static void setup() {
        server = serverBuilder(REDIS_PORT).setting("notify-keyspace-events AK")
                .setting("loadmodule /Users/jruaux/git/RediSearch/build/redisearch.so").build();
        server.start();
        client = RediSearchClient.create(RedisURI.create(REDIS_HOST, REDIS_PORT));
        connection = client.connect();
    }

    protected static RedisServerBuilder serverBuilder(int port) {
        RedisExecProvider provider = RedisExecProvider.defaultProvider().override(OS.MAC_OS_X,
                "/usr/local/bin/redis-server");
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
        try (InputStream inputStream = BaseTest.class.getResourceAsStream("/commands/" + filename + ".txt")) {
            return runCommand(removePreamble(IOUtils.toString(inputStream, Charset.defaultCharset())), args);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    protected int runCommand(String command, Object... args) throws Exception {
        return new Riot().execute(CommandLineUtils.translateCommandline(String.format(command, args)));
    }

    private String removePreamble(String command) {
        if (command.startsWith(COMMAND_PREAMBLE)) {
            return command.substring(COMMAND_PREAMBLE.length());
        }
        return command;
    }

}
