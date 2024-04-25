package com.redis.riot.cli;

import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.redis.riot.cli.RedisReaderArgs.ReadFromEnum;
import com.redis.riot.core.RiotVersion;

import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ReplicateTargetArgs {

	@ArgGroup(exclusive = false)
	private RedisWriterArgs writerArgs = new RedisWriterArgs();

	@Option(names = "--target-read-from", description = "Which target cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
	private ReadFromEnum readFrom;

	@Option(names = "--target-uri", description = "Target server URI.", paramLabel = "<uri>")
	private String uri;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-client", description = "Client name used to connect to target Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RiotVersion.riotVersion();

	@Option(names = "--target-cluster", description = "Enable target cluster mode.")
	private boolean cluster;

	@Option(names = "--target-auto-reconnect", defaultValue = "true", fallbackValue = "true", negatable = true, description = "Automatically reconnect to target on connection loss. True by default.", hidden = true)
	private boolean autoReconnect = true;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion;

	public RedisWriterArgs getWriterArgs() {
		return writerArgs;
	}

	public void setWriterArgs(RedisWriterArgs writerArgs) {
		this.writerArgs = writerArgs;
	}

	public ReadFromEnum getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFromEnum readFrom) {
		this.readFrom = readFrom;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public boolean isAutoReconnect() {
		return autoReconnect;
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	public RedisURI redisURI() {
		RedisURI.Builder builder = RedisURI.builder(RedisURI.create(uri));
		if (!ObjectUtils.isEmpty(password)) {
			builder.withPassword(password);
		}
		if (StringUtils.hasLength(clientName)) {
			builder.withClientName(clientName);
		}
		return builder.build();
	}

}
