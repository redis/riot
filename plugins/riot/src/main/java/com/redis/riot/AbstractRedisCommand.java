package com.redis.riot;

import java.lang.reflect.Method;

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.RedisClientBuilder.RedisURIClient;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.EvaluationContextArgs;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import picocli.CommandLine.ArgGroup;

abstract class AbstractRedisCommand extends AbstractJobCommand<Main> {

	private static final String CONTEXT_VAR_REDIS = "redis";

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private RedisArgs redisArgs = new RedisArgs();

	protected RedisURIClient redisURIClient;
	protected StatefulRedisModulesConnection<String, String> redisConnection;
	protected RedisModulesCommands<String, String> redisCommands;

	public void copyTo(AbstractRedisCommand target) {
		super.copyTo(target);
		target.redisArgs = redisArgs;
		target.redisURIClient = redisURIClient;
		target.redisConnection = redisConnection;
		target.redisCommands = redisCommands;
	}

	@Override
	protected void setup() {
		super.setup();
		if (redisURIClient == null) {
			redisURIClient = redisArgs.redisURIClient();
		}
		redisConnection = RedisModulesUtils.connection(redisURIClient.getClient());
		redisCommands = redisConnection.sync();
	}

	@Override
	protected void execute() throws Exception {
		super.execute();
		redisCommands = null;
		redisConnection.close();
		redisConnection = null;
		redisURIClient.close();
		redisURIClient = null;
	}

	protected StandardEvaluationContext evaluationContext(EvaluationContextArgs args) {
		StandardEvaluationContext evaluationContext = args.evaluationContext();
		configure(evaluationContext);
		return evaluationContext;
	}

	protected void configure(StandardEvaluationContext context) {
		context.setVariable(CONTEXT_VAR_REDIS, redisCommands);
		Method method;
		try {
			method = GeoLocation.class.getDeclaredMethod("toString", String.class, String.class);
		} catch (Exception e) {
			throw new UnsupportedOperationException("Could not get GeoLocation method", e);
		}
		context.registerFunction("geo", method);
	}

	@Override
	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> ftStep = super.faultTolerant(step);
		ftStep.skip(RedisCommandExecutionException.class);
		ftStep.noRetry(RedisCommandExecutionException.class);
		ftStep.noSkip(RedisCommandTimeoutException.class);
		ftStep.retry(RedisCommandTimeoutException.class);
		return ftStep;
	}

	protected void configure(RedisItemReader<?, ?, ?> reader) {
		super.configure(reader);
		reader.setClient(redisURIClient.getClient());
		reader.setDatabase(redisURIClient.getUri().getDatabase());
		reader.setPoolSize(redisArgs.getPoolSize());
	}

	protected void configure(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(redisURIClient.getClient());
	}

	public RedisURIClient getRedisURIClient() {
		return redisURIClient;
	}

	public void setRedisURIClient(RedisURIClient redisURIClient) {
		this.redisURIClient = redisURIClient;
	}

	public StatefulRedisModulesConnection<String, String> getRedisConnection() {
		return redisConnection;
	}

	public void setRedisConnection(StatefulRedisModulesConnection<String, String> redisConnection) {
		this.redisConnection = redisConnection;
	}

	public RedisModulesCommands<String, String> getRedisCommands() {
		return redisCommands;
	}

	public void setRedisCommands(RedisModulesCommands<String, String> redisCommands) {
		this.redisCommands = redisCommands;
	}

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs args) {
		this.redisArgs = args;
	}

}
