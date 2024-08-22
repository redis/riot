package com.redis.riot;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.item.redis.RedisItemReader;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractTargetCommand extends AbstractJobCommand<TargetRedisExecutionContext> {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	@ArgGroup(exclusive = false, heading = "TLS options%n")
	private SslArgs sslArgs = new SslArgs();

	@Parameters(arity = "1", index = "0", description = "Source server URI.", paramLabel = "SOURCE")
	private RedisURI sourceRedisURI;

	@ArgGroup(exclusive = false)
	private SourceRedisURIArgs sourceRedisURIArgs = new SourceRedisURIArgs();

	@ArgGroup(exclusive = false)
	private SourceRedisClientArgs sourceRedisClientArgs = new SourceRedisClientArgs();

	@Parameters(arity = "1", index = "1", description = "Target server URI.", paramLabel = "TARGET")
	private RedisURI targetRedisURI;

	@ArgGroup(exclusive = false)
	private TargetRedisURIArgs targetRedisURIArgs = new TargetRedisURIArgs();

	@ArgGroup(exclusive = false)
	private TargetRedisClientArgs targetRedisClientArgs = new TargetRedisClientArgs();

	@ArgGroup(exclusive = false)
	private RedisReaderArgs sourceRedisReaderArgs = new RedisReaderArgs();

	@Option(names = "--target-read-from", description = "Which target Redis cluster nodes to read from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
	private RedisReadFrom targetReadFrom;

	@Override
	protected TargetRedisExecutionContext newExecutionContext() {
		TargetRedisExecutionContext context = new TargetRedisExecutionContext();
		context.setSourceRedisContext(sourceRedisContext());
		context.setTargetRedisContext(targetRedisContext());
		return context;
	}

	private RedisContext sourceRedisContext() {
		RedisURI redisURI = sourceRedisURI();
		log.info("Creating source Redis context with uri={} {} {}", redisURI, sslArgs, sourceRedisClientArgs);
		RedisContext context = new RedisContext();
		context.setAutoReconnect(sourceRedisClientArgs.isAutoReconnect());
		context.setCluster(sourceRedisClientArgs.isCluster());
		context.setPoolSize(sourceRedisClientArgs.getPoolSize());
		context.setProtocolVersion(sourceRedisClientArgs.getProtocolVersion());
		context.setSslOptions(sslArgs.sslOptions());
		context.setUri(redisURI);
		return context;
	}

	private RedisURI sourceRedisURI() {
		return sourceRedisURIArgs.redisURI(sourceRedisURI);
	}

	private RedisContext targetRedisContext() {
		RedisURI redisURI = targetRedisURI();
		log.info("Creating target Redis context with uri={} {} {}", redisURI, sslArgs, targetRedisClientArgs);
		RedisContext context = new RedisContext();
		context.setAutoReconnect(targetRedisClientArgs.isAutoReconnect());
		context.setCluster(targetRedisClientArgs.isCluster());
		context.setPoolSize(targetRedisClientArgs.getPoolSize());
		context.setProtocolVersion(targetRedisClientArgs.getProtocolVersion());
		context.setSslOptions(sslArgs.sslOptions());
		context.setUri(redisURI);
		return context;
	}

	private RedisURI targetRedisURI() {
		return targetRedisURIArgs.redisURI(targetRedisURI);
	}

	protected void configureSourceReader(TargetRedisExecutionContext context, RedisItemReader<?, ?, ?> reader) {
		sourceRedisReaderArgs.configure(reader);
		context.configureSourceReader(reader);
	}

	protected void configureTargetReader(TargetRedisExecutionContext context, RedisItemReader<?, ?, ?> reader) {
		if (targetReadFrom != null) {
			reader.setReadFrom(targetReadFrom.getReadFrom());
		}
		context.configureTargetReader(reader);
	}

	public SourceRedisURIArgs getSourceRedisURIArgs() {
		return sourceRedisURIArgs;
	}

	public void setSourceRedisURIArgs(SourceRedisURIArgs args) {
		this.sourceRedisURIArgs = args;
	}

	public SourceRedisClientArgs getSourceRedisClientArgs() {
		return sourceRedisClientArgs;
	}

	public void setSourceRedisClientArgs(SourceRedisClientArgs args) {
		this.sourceRedisClientArgs = args;
	}

	public TargetRedisURIArgs getTargetRedisURIArgs() {
		return targetRedisURIArgs;
	}

	public void setTargetRedisURIArgs(TargetRedisURIArgs args) {
		this.targetRedisURIArgs = args;
	}

	public TargetRedisClientArgs getTargetRedisClientArgs() {
		return targetRedisClientArgs;
	}

	public void setTargetRedisClientArgs(TargetRedisClientArgs args) {
		this.targetRedisClientArgs = args;
	}

	public RedisReaderArgs getSourceRedisReaderArgs() {
		return sourceRedisReaderArgs;
	}

	public void setSourceRedisReaderArgs(RedisReaderArgs args) {
		this.sourceRedisReaderArgs = args;
	}

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs sslArgs) {
		this.sslArgs = sslArgs;
	}

	public RedisReadFrom getTargetReadFrom() {
		return targetReadFrom;
	}

	public void setTargetReadFrom(RedisReadFrom targetReadFrom) {
		this.targetReadFrom = targetReadFrom;
	}

	public RedisURI getSourceRedisURI() {
		return sourceRedisURI;
	}

	public void setSourceRedisURI(RedisURI sourceRedisURI) {
		this.sourceRedisURI = sourceRedisURI;
	}

	public RedisURI getTargetRedisURI() {
		return targetRedisURI;
	}

	public void setTargetRedisURI(RedisURI targetRedisURI) {
		this.targetRedisURI = targetRedisURI;
	}

}
