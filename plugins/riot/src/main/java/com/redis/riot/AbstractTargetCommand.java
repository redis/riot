package com.redis.riot;

import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.RedisClientBuilder.RedisURIClient;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Parameters;

public abstract class AbstractTargetCommand extends AbstractRedisCommand {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	private static final String SOURCE_VAR = "source";
	private static final String TARGET_VAR = "target";

	@Parameters(arity = "1", index = "0", description = "Source server URI.", paramLabel = "SOURCE")
	private RedisURI sourceRedisURI;

	@ArgGroup(exclusive = false)
	private SourceRedisArgs sourceRedisArgs = new SourceRedisArgs();

	@Parameters(arity = "1", index = "1", description = "Target server URI.", paramLabel = "TARGET")
	private RedisURI targetRedisURI;

	@ArgGroup(exclusive = false)
	private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

	@ArgGroup(exclusive = false)
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	protected RedisURIClient targetRedisURIClient;

	protected <T extends RedisItemWriter<?, ?, ?>> T configure(T writer) {
		writer.setClient(targetRedisURIClient.getClient());
		return writer;
	}

	@Override
	protected RedisURIClient redisURIClient() {
		RedisClientBuilder builder = sourceRedisArgs.configure(redisClientBuilder());
		builder.uri(sourceRedisURI);
		log.info("Creating source Redis client with {}", builder);
		return builder.build();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		if (targetRedisURIClient == null) {
			RedisClientBuilder builder = targetRedisArgs.configure(redisClientBuilder());
			builder.uri(targetRedisURI);
			log.info("Creating target Redis client with {}", builder);
			targetRedisURIClient = builder.build();
		}
	}

	@Override
	protected void shutdown() {
		if (targetRedisURIClient != null) {
			log.info("Shutting down target Redis client");
			targetRedisURIClient.close();
			targetRedisURIClient = null;
		}
		super.shutdown();
	}

	@Override
	protected StandardEvaluationContext evaluationContext(ProcessorArgs args) {
		StandardEvaluationContext context = super.evaluationContext(args);
		log.info("Setting evaluation context variable {} = {}", SOURCE_VAR, client.getUri());
		context.setVariable(SOURCE_VAR, client.getUri());
		log.info("Setting evaluation context variable {} = {}", TARGET_VAR, targetRedisURIClient.getUri());
		context.setVariable(TARGET_VAR, targetRedisURIClient.getUri());
		return context;
	}

	@Override
	protected <K, V, T> RedisItemReader<K, V, T> configure(RedisItemReader<K, V, T> reader) {
		log.info("Configuring Redis reader with {}", redisReaderArgs);
		redisReaderArgs.configure(reader);
		return super.configure(reader);
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs args) {
		this.redisReaderArgs = args;
	}

	public TargetRedisArgs getTargetRedisArgs() {
		return targetRedisArgs;
	}

	public void setTargetRedisArgs(TargetRedisArgs args) {
		this.targetRedisArgs = args;
	}

	public RedisURI getSourceRedisURI() {
		return sourceRedisURI;
	}

	public void setSourceRedisURI(RedisURI sourceRedisURI) {
		this.sourceRedisURI = sourceRedisURI;
	}

	public SourceRedisArgs getSourceRedisArgs() {
		return sourceRedisArgs;
	}

	public void setSourceRedisArgs(SourceRedisArgs sourceRedisArgs) {
		this.sourceRedisArgs = sourceRedisArgs;
	}

	public RedisURI getTargetRedisURI() {
		return targetRedisURI;
	}

	public void setTargetRedisURI(RedisURI targetRedisURI) {
		this.targetRedisURI = targetRedisURI;
	}

}
