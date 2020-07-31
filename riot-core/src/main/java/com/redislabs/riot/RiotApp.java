package com.redislabs.riot;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Getter;
import org.springframework.batch.item.redis.support.ConnectionPoolConfig;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import picocli.CommandLine;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@CommandLine.Command(usageHelpAutoWidth = true, sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = HiddenGenerateCompletion.class, abbreviateSynopsis = true)
public class RiotApp implements Runnable {

    private static final String ROOT_LOGGER = "";

    @CommandLine.Option(names = {"--help"}, usageHelp = true, description = "Show this help message and exit.")
    private boolean helpRequested;
    @CommandLine.Option(names = {"-V", "--version"}, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;
    @Getter
    @CommandLine.Option(names = {"-q", "--quiet"}, description = "Log errors only")
    private boolean quiet;
    @Getter
    @CommandLine.Option(names = {"-d", "--debug"}, description = "Log in debug mode (includes normal stacktrace)")
    private boolean debug;
    @Getter
    @CommandLine.Option(names = {"-i", "--info"}, description = "Set log level to info")
    private boolean info;
    @CommandLine.ArgGroup(heading = "Redis connection options%n", exclusive = false)
    private RedisConnectionOptions redis = new RedisConnectionOptions();

    public RedisConnectionOptions getRedisConnectionOptions() {
        return redis;
    }

    public int execute(String... args) {
        try {
            CommandLine commandLine = commandLine();
            CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
            InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
            LogManager.getLogManager().reset();
            Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(Level.ALL);
            handler.setFormatter(new OneLineLogFormat(isDebug()));
            activeLogger.addHandler(handler);
            Logger.getLogger(ROOT_LOGGER).setLevel(rootLoggingLevel());
            Logger logger = Logger.getLogger("com.redislabs");
            logger.setLevel(packageLoggingLevel());
            return commandLine.getExecutionStrategy().execute(parseResult);
        } catch (CommandLine.PicocliException e) {
            System.err.println(e.getMessage());
            return 1;
        }
    }

    public CommandLine commandLine() {
        CommandLine commandLine = new CommandLine(this);
        registerConverters(commandLine);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return commandLine;
    }

    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    private Level packageLoggingLevel() {
        if (isQuiet()) {
            return Level.OFF;
        }
        if (isInfo()) {
            return Level.FINE;
        }
        if (isDebug()) {
            return Level.FINEST;
        }
        return Level.INFO;
    }

    private Level rootLoggingLevel() {
        if (isQuiet()) {
            return Level.OFF;
        }
        if (isInfo()) {
            return Level.INFO;
        }
        if (isDebug()) {
            return Level.FINE;
        }
        return Level.SEVERE;
    }

    public <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder) {
        return configure(builder, redis);
    }

    public <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder) {
        return configure(builder, redis);
    }

    public <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return builder.redisURI(redis.getRedisURI()).cluster(redis.isCluster()).clientResources(redis.getClientResources()).clientOptions(redis.getClientOptions()).poolConfig(ConnectionPoolConfig.builder().maxTotal(redis.getPoolMaxTotal()).build());
    }

    public <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return builder.redisURI(redis.getRedisURI()).clientResources(redis.getClientResources()).clientOptions(redis.getClientOptions()).poolConfig(org.springframework.batch.item.redisearch.support.ConnectionPoolConfig.builder().maxTotal(redis.getPoolMaxTotal()).build());
    }

    public StatefulConnection<String, String> connection() {
        RedisConnectionBuilder<?> connectionBuilder = new RedisConnectionBuilder<>();
        configure(connectionBuilder);
        return connectionBuilder.connection();
    }

    public BaseRedisCommands<String, String> sync(StatefulConnection<String, String> connection) {
        RedisConnectionBuilder<?> connectionBuilder = new RedisConnectionBuilder<>();
        configure(connectionBuilder);
        return connectionBuilder.sync().apply(connection);
    }

    public BaseRedisAsyncCommands<String, String> async(StatefulConnection<String, String> connection) {
        RedisConnectionBuilder<?> connectionBuilder = new RedisConnectionBuilder<>();
        configure(connectionBuilder);
        return connectionBuilder.async().apply(connection);
    }

    public StatefulRediSearchConnection<String, String> rediSearchConnection() {
        RediSearchConnectionBuilder<?> connectionBuilder = new RediSearchConnectionBuilder<>();
        configure(connectionBuilder);
        return connectionBuilder.connection();
    }
}
