package com.redis.riot.core;

import java.text.SimpleDateFormat;
import java.time.Duration;

import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

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
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyCollector;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

public abstract class AbstractRiotRunnable implements Runnable {

    public static final String DATE_VARIABLE_NAME = "date";

    public static final String REDIS_VARIABLE_NAME = "redis";

    private RedisOptions redisOptions = new RedisOptions();

    private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();

    public void setEvaluationContextOptions(EvaluationContextOptions options) {
        this.evaluationContextOptions = options;
    }

    public void setRedisOptions(RedisOptions options) {
        this.redisOptions = options;
    }

    @Override
    public void run() {
        try (RiotContext context = createExecutionContext()) {
            execute(context);
        }
    }

    protected RiotContext createExecutionContext() {
        RedisContext redisContext = redisContext(redisOptions);
        StandardEvaluationContext evaluationContext = evaluationContext(redisContext);
        return new RiotContext(redisContext, evaluationContext);
    }

    protected RedisContext redisContext(RedisOptions options) {
        RedisURI redisURI = redisURI(options.getUriOptions());
        AbstractRedisClient client = client(redisURI, options);
        return new RedisContext(redisURI, client);
    }

    private StandardEvaluationContext evaluationContext(RedisContext redisContext) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable(DATE_VARIABLE_NAME, new SimpleDateFormat(evaluationContextOptions.getDateFormat()));
        context.setVariable(REDIS_VARIABLE_NAME, redisContext.getConnection().sync());
        if (!CollectionUtils.isEmpty(evaluationContextOptions.getVariables())) {
            evaluationContextOptions.getVariables().forEach(context::setVariable);
        }
        if (!CollectionUtils.isEmpty(evaluationContextOptions.getExpressions())) {
            evaluationContextOptions.getExpressions().forEach((k, v) -> context.setVariable(k, v.getValue(context)));
        }
        return context;
    }

    protected abstract void execute(RiotContext executionContext);

    public RedisURI redisURI(RedisUriOptions options) {
        RedisURI.Builder builder = redisURIBuilder(options);
        if (options.getDatabase() > 0) {
            builder.withDatabase(options.getDatabase());
        }
        if (StringUtils.hasLength(options.getClientName())) {
            builder.withClientName(options.getClientName());
        }
        if (!ObjectUtils.isEmpty(options.getPassword())) {
            if (StringUtils.hasLength(options.getUsername())) {
                builder.withAuthentication(options.getUsername(), options.getPassword());
            } else {
                builder.withPassword(options.getPassword());
            }
        }
        if (options.isTls()) {
            builder.withSsl(options.isTls());
            builder.withVerifyPeer(options.getVerifyPeer());
        }
        if (options.getTimeout() != null) {
            builder.withTimeout(options.getTimeout());
        }
        return builder.build();
    }

    private RedisURI.Builder redisURIBuilder(RedisUriOptions options) {
        if (StringUtils.hasLength(options.getUri())) {
            return RedisURI.builder(RedisURI.create(options.getUri()));
        }
        if (StringUtils.hasLength(options.getSocket())) {
            return RedisURI.Builder.socket(options.getSocket());
        }
        return RedisURI.Builder.redis(options.getHost(), options.getPort());
    }

    private AbstractRedisClient client(RedisURI redisURI, RedisOptions options) {
        ClientResources resources = clientResources(options);
        if (options.isCluster()) {
            RedisModulesClusterClient client = RedisModulesClusterClient.create(resources, redisURI);
            client.setOptions(clientOptions(ClusterClientOptions.builder(), options.getClientOptions()).build());
            return client;
        }
        RedisModulesClient client = RedisModulesClient.create(resources, redisURI);
        client.setOptions(clientOptions(ClientOptions.builder(), options.getClientOptions()).build());
        return client;
    }

    private <B extends ClientOptions.Builder> B clientOptions(B builder, RedisClientOptions options) {
        builder.autoReconnect(options.isAutoReconnect());
        builder.sslOptions(sslOptions(options.getSslOptions()));
        builder.protocolVersion(options.getProtocolVersion());
        return builder;
    }

    public SslOptions sslOptions(RedisSslOptions options) {
        Builder ssl = SslOptions.builder();
        if (options.getKey() != null) {
            ssl.keyManager(options.getKeyCert(), options.getKey(), options.getKeyPassword());
        }
        if (options.getKeystore() != null) {
            ssl.keystore(options.getKeystore(), options.getKeystorePassword());
        }
        if (options.getTruststore() != null) {
            ssl.truststore(Resource.from(options.getTruststore()), options.getTruststorePassword());
        }
        if (options.getTrustedCerts() != null) {
            ssl.trustManager(options.getTrustedCerts());
        }
        return ssl.build();
    }

    private ClientResources clientResources(RedisOptions options) {
        DefaultClientResources.Builder builder = DefaultClientResources.builder();
        if (options.getMetricsStep() != null) {
            builder.commandLatencyRecorder(commandLatencyRecorder());
            builder.commandLatencyPublisherOptions(commandLatencyPublisherOptions(options.getMetricsStep()));
        }
        return builder.build();
    }

    private EventPublisherOptions commandLatencyPublisherOptions(Duration step) {
        return DefaultEventPublisherOptions.builder().eventEmitInterval(step).build();
    }

    private CommandLatencyRecorder commandLatencyRecorder() {
        return CommandLatencyCollector.create(DefaultCommandLatencyCollectorOptions.builder().enable().build());
    }

}
