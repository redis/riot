package com.redis.riot;

import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Parameters;

public abstract class AbstractRedisTargetExportCommand extends AbstractExportCommand {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;
	private static final String VAR_TARGET = "target";

	@ArgGroup(exclusive = false, heading = "TLS options%n")
	private SslArgs sslArgs = new SslArgs();

	@Parameters(arity = "1", index = "0", description = "Source server URI or endpoint in the form host:port.", paramLabel = "SOURCE")
	private RedisURI sourceRedisUri;

	@ArgGroup(exclusive = false)
	private SourceRedisArgs sourceRedisArgs = new SourceRedisArgs();

	@Parameters(arity = "1", index = "1", description = "Target server URI or endpoint in the form host:port.", paramLabel = "TARGET")
	private RedisURI targetRedisUri;

	@ArgGroup(exclusive = false)
	private TargetRedisArgs targetRedisArgs = new TargetRedisArgs();

	private RedisContext targetRedisContext;

	@Override
	protected void execute() throws Exception {
		targetRedisContext = targetRedisContext();
		targetRedisContext.afterPropertiesSet();
		try {
			super.execute();
		} finally {
			targetRedisContext.close();
		}
	}

	@Override
	protected RedisContext sourceRedisContext() {
		log.info("Creating source Redis context with {} {} {}", sourceRedisUri, sourceRedisArgs, sslArgs);
		RedisContext context = sourceRedisArgs.redisContext(sourceRedisUri);
		context.sslOptions(sslArgs.sslOptions());
		return context;
	}

	private RedisContext targetRedisContext() {
		log.info("Creating target Redis context with {} {} {}", targetRedisUri, targetRedisArgs, sslArgs);
		RedisContext context = targetRedisArgs.redisContext(targetRedisUri);
		context.sslOptions(sslArgs.sslOptions());
		return context;
	}

	@Override
	protected void configure(StandardEvaluationContext context) {
		super.configure(context);
		context.setVariable(VAR_TARGET, targetRedisContext.getConnection().sync());
	}

	protected void configureTargetRedisReader(RedisItemReader<?, ?> reader) {
		configureAsyncReader(reader);
		targetRedisContext.configure(reader);
	}

	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		targetRedisContext.configure(writer);
	}

	public RedisURI getSourceRedisUri() {
		return sourceRedisUri;
	}

	public void setSourceRedisUri(RedisURI sourceRedisUri) {
		this.sourceRedisUri = sourceRedisUri;
	}

	public SourceRedisArgs getSourceRedisArgs() {
		return sourceRedisArgs;
	}

	public void setSourceRedisArgs(SourceRedisArgs sourceRedisArgs) {
		this.sourceRedisArgs = sourceRedisArgs;
	}

	public RedisURI getTargetRedisUri() {
		return targetRedisUri;
	}

	public void setTargetRedisUri(RedisURI targetRedisUri) {
		this.targetRedisUri = targetRedisUri;
	}

	public TargetRedisArgs getTargetRedisArgs() {
		return targetRedisArgs;
	}

	public void setTargetRedisArgs(TargetRedisArgs targetRedisArgs) {
		this.targetRedisArgs = targetRedisArgs;
	}

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs sslArgs) {
		this.sslArgs = sslArgs;
	}

}
