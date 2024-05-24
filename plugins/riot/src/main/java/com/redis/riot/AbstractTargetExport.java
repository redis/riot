package com.redis.riot;

import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.RedisClientBuilder.RedisURIClient;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractTargetExport extends AbstractExport {

	private static final String SOURCE_VAR = "source";
	private static final String TARGET_VAR = "target";

	@ArgGroup(exclusive = false, heading = "Target Redis options%n")
	private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

	private RedisURIClient targetRedisURIClient;

	public void copyTo(AbstractTargetExport target) {
		super.copyTo(target);
		target.targetRedisArgs = targetRedisArgs;
		target.targetRedisURIClient = targetRedisURIClient;
	}

	@Override
	protected void setup() {
		super.setup();
		log.info("Creating target Redis client with {} and {}", targetRedisArgs, getRedisArgs().getSslArgs());
		targetRedisURIClient = targetRedisArgs.redisURIClient(getRedisArgs().getSslArgs());
	}

	@Override
	protected void execute() throws Exception {
		super.execute();
		log.info("Shutting down target Redis client");
		targetRedisURIClient.close();
		targetRedisURIClient = null;
	}

	@Override
	protected void configure(StandardEvaluationContext context) {
		log.info("Setting evaluation context variable {} = {}", SOURCE_VAR, redisURIClient.getUri());
		context.setVariable(SOURCE_VAR, redisURIClient.getUri());
		log.info("Setting evaluation context variable {} = {}", TARGET_VAR, targetRedisURIClient.getUri());
		context.setVariable(TARGET_VAR, targetRedisURIClient.getUri());
	}

	protected void configureTarget(RedisItemReader<?, ?, ?> reader) {
		reader.setClient(targetRedisURIClient.getClient());
		log.info("Configuring target Redis reader with pool size {}", targetRedisArgs.getPoolSize());
		reader.setPoolSize(targetRedisArgs.getPoolSize());
	}

	protected void configureTarget(RedisItemWriter<?, ?, ?> writer) {
		writer.setClient(targetRedisURIClient.getClient());
		log.info("Configuring target Redis writer with pool size {}", targetRedisArgs.getPoolSize());
		writer.setPoolSize(targetRedisArgs.getPoolSize());
	}

	public RedisURIClient getTargetRedisURIClient() {
		return targetRedisURIClient;
	}

	public void setTargetRedisURIClient(RedisURIClient client) {
		this.targetRedisURIClient = client;
	}

	public TargetRedisArgs getTargetRedisArgs() {
		return targetRedisArgs;
	}

	public void setTargetRedisArgs(TargetRedisArgs args) {
		this.targetRedisArgs = args;
	}

}
