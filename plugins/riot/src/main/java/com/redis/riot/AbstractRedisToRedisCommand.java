package com.redis.riot;

import com.redis.lettucemod.RedisURIBuilder;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.item.redis.RedisItemReader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractRedisToRedisCommand extends AbstractJobCommand {

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

	protected RedisContext sourceRedisContext;
	protected RedisContext targetRedisContext;

	private RedisURI redisURI(RedisURI uri, String username, char[] password, boolean insecure) {
		RedisURIBuilder builder = new RedisURIBuilder();
		builder.password(password);
		builder.uri(uri);
		builder.username(username);
		if (insecure) {
			builder.verifyMode(SslVerifyMode.NONE);
		}
		return builder.build();
	}

	@Override
	protected void execute() throws Exception {
		sourceRedisContext = sourceRedisContext();
		targetRedisContext = targetRedisContext();
		try {
			super.execute();
		} finally {
			targetRedisContext.close();
			sourceRedisContext.close();
		}
	}

	private RedisContext sourceRedisContext() {
		log.info("Creating source Redis URI with uri={} {}", sourceRedisURI, sourceRedisURIArgs);
		RedisURI sourceURI = redisURI(sourceRedisURI, sourceRedisURIArgs.getUsername(),
				sourceRedisURIArgs.getPassword(), sourceRedisURIArgs.isInsecure());
		log.info("Creating source Redis context with uri={} {} {}", sourceURI, sourceRedisClientArgs, sslArgs);
		return RedisContext.create(sourceURI, sourceRedisClientArgs.isCluster(),
				sourceRedisClientArgs.isAutoReconnect(), sourceRedisClientArgs.getProtocolVersion(),
				sslArgs.sslOptions());
	}

	private RedisContext targetRedisContext() {
		log.info("Creating target Redis URI with uri={} {}", targetRedisURI, targetRedisURIArgs);
		RedisURI targetURI = redisURI(targetRedisURI, targetRedisURIArgs.getUsername(),
				targetRedisURIArgs.getPassword(), targetRedisURIArgs.isInsecure());
		log.info("Creating target Redis context with uri={} {} {}", targetURI, targetRedisClientArgs, sslArgs);
		return RedisContext.create(targetURI, targetRedisClientArgs.isCluster(),
				targetRedisClientArgs.isAutoReconnect(), targetRedisClientArgs.getProtocolVersion(),
				sslArgs.sslOptions());

	}

	protected void configureSourceReader(RedisItemReader<?, ?, ?> reader) {
		configureAsyncReader(reader);
		sourceRedisContext.configure(reader);
		log.info("Configuring source Redis reader with {}", sourceRedisReaderArgs);
		sourceRedisReaderArgs.configure(reader);
	}

	protected void configureTargetReader(RedisItemReader<?, ?, ?> reader) {
		configureAsyncReader(reader);
		targetRedisContext.configure(reader);
		if (targetReadFrom != null) {
			log.info("Configuring target Redis reader with read-from {}", targetReadFrom);
			reader.setReadFrom(targetReadFrom.getReadFrom());
		}
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
