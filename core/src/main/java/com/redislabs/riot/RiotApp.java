package com.redislabs.riot;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.event.DefaultEventPublisherOptions;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import lombok.Getter;
import org.springframework.batch.item.redis.support.ConnectionPoolConfig;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import picocli.CommandLine;

import java.io.File;
import java.time.Duration;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

@CommandLine.Command(mixinStandardHelpOptions = true, usageHelpAutoWidth = true, sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = HiddenGenerateCompletion.class)
public class RiotApp implements Runnable {

    @Getter
    @CommandLine.Option(names = {"-q", "--quiet"}, description = "Log errors only")
    private boolean quiet;
    @Getter
    @CommandLine.Option(names = {"-d", "--debug"}, description = "Log in debug mode (includes normal stacktrace)")
    private boolean debug;
    @Getter
    @CommandLine.Option(names = {"-i", "--info"}, description = "Set log level to info")
    private boolean info;
    @CommandLine.Option(names = "--ks", description = "Path to keystore", paramLabel = "<file>", hidden = true)
    private File keystore;
    @CommandLine.Option(names = "--ks-password", arity = "0..1", interactive = true, description = "Keystore password", paramLabel = "<pwd>", hidden = true)
    private String keystorePassword;
    @CommandLine.Option(names = "--ts", description = "Path to truststore", paramLabel = "<file>", hidden = true)
    private File truststore;
    @CommandLine.Option(names = "--ts-password", arity = "0..1", interactive = true, description = "Truststore password", paramLabel = "<pwd>", hidden = true)
    private String truststorePassword;
    @CommandLine.ArgGroup(heading = "Redis connection options%n", exclusive = false)
    private RedisConnectionOptions redis = new RedisConnectionOptions();

    public int execute(String... args) {
        CommandLine commandLine = new CommandLine(this);
        registerConverters(commandLine);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        try {
            CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
            InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
            LogManager.getLogManager().reset();
            Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(Level.ALL);
            handler.setFormatter(new OneLineLogFormat(isDebug()));
            activeLogger.addHandler(handler);
            Logger.getLogger(ROOT_LOGGER).setLevel(rootLoggingLevel());
            Logger.getLogger("com.redislabs").setLevel(packageLoggingLevel());
            return commandLine.getExecutionStrategy().execute(parseResult);
        } catch (CommandLine.PicocliException e) {
            System.err.println(e.getMessage());
            return 1;
        }
    }

    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(RedisURI.class, new RedisURIConverter());
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    private ClusterClientOptions clientOptions() {
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
        return ClusterClientOptions.builder().sslOptions(sslOptionsBuilder.build()).build();
    }

    private static final String ROOT_LOGGER = "";

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
        return builder.redisURI(redis.getRedisURI()).cluster(redis.isCluster()).clientResources(clientResources(redis)).clientOptions(clientOptions()).poolConfig(ConnectionPoolConfig.builder().maxTotal(redis.getPoolMaxTotal()).build());
    }

    public <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return builder.redisURI(redis.getRedisURI()).clientResources(clientResources(redis)).clientOptions(clientOptions()).poolConfig(org.springframework.batch.item.redisearch.support.ConnectionPoolConfig.builder().maxTotal(redis.getPoolMaxTotal()).build());
    }

    private ClientResources clientResources(RedisConnectionOptions redis) {
        if (redis.isShowMetrics()) {
            DefaultClientResources.Builder clientResourcesBuilder = DefaultClientResources.builder();
            clientResourcesBuilder.commandLatencyCollectorOptions(DefaultCommandLatencyCollectorOptions.builder().enable().build());
            clientResourcesBuilder.commandLatencyPublisherOptions(DefaultEventPublisherOptions.builder().eventEmitInterval(Duration.ofSeconds(1)).build());
            ClientResources resources = clientResourcesBuilder.build();
            resources.eventBus().get().filter(redisEvent -> redisEvent instanceof CommandLatencyEvent).cast(CommandLatencyEvent.class).subscribe(e -> System.out.println(e.getLatencies()));
            return clientResourcesBuilder.build();
        }
        return null;
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
