package com.redis.riot.core;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;
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

	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private static final String DATE_VAR = "date";
	private static final String REDIS_VAR = "redis";

	private RedisOptions redisOptions = new RedisOptions();
	private String dateFormat = DEFAULT_DATE_FORMAT;
	private Map<String, Expression> varExpressions = new LinkedHashMap<>();
	private Map<String, Object> vars = new LinkedHashMap<>();

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String format) {
		this.dateFormat = format;
	}

	public Map<String, Expression> getVarExpressions() {
		return varExpressions;
	}

	public void setVarExpressions(Map<String, Expression> expressions) {
		this.varExpressions = expressions;
	}

	public Map<String, Object> getVars() {
		return vars;
	}

	public void setVars(Map<String, Object> variables) {
		this.vars = variables;
	}

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public void setRedisOptions(RedisOptions options) {
		this.redisOptions = options;
	}

	@Override
	public void run() {
		try (RiotContext context = createContext()) {
			execute(context);
		}
	}

	protected RiotContext createContext() {
		RedisContext redisContext = redisContext(redisOptions);
		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.setVariable(DATE_VAR, new SimpleDateFormat(dateFormat));
		evaluationContext.setVariable(REDIS_VAR, redisContext.getConnection().sync());
		if (!CollectionUtils.isEmpty(vars)) {
			vars.forEach(evaluationContext::setVariable);
		}
		if (!CollectionUtils.isEmpty(varExpressions)) {
			varExpressions.forEach((k, v) -> evaluationContext.setVariable(k, v.getValue(evaluationContext)));
		}
		return new RiotContext(redisContext, evaluationContext);
	}

	protected RedisContext redisContext(RedisOptions options) {
		RedisURI redisURI = redisURI(options);
		AbstractRedisClient client = client(redisURI, options);
		return new RedisContext(redisURI, client);
	}

	protected abstract void execute(RiotContext executionContext);

	public RedisURI redisURI(RedisOptions options) {
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

	private RedisURI.Builder redisURIBuilder(RedisOptions options) {
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
			client.setOptions(clientOptions(ClusterClientOptions.builder(), options).build());
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(resources, redisURI);
		client.setOptions(clientOptions(ClientOptions.builder(), options).build());
		return client;
	}

	private <B extends ClientOptions.Builder> B clientOptions(B builder, RedisOptions options) {
		builder.autoReconnect(options.isAutoReconnect());
		builder.sslOptions(sslOptions(options));
		builder.protocolVersion(options.getProtocolVersion());
		return builder;
	}

	public SslOptions sslOptions(RedisOptions options) {
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
