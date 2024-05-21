package com.redis.riot.cli;

import org.springframework.util.StringUtils;

import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RiotVersion;

import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ReplicateSourceRedisArgs {

	@Option(names = "--source-user", description = "Source ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--source-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the source server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--source-client", description = "Client name used to connect to source Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RiotVersion.riotVersion();

	@Option(names = "--source-cluster", description = "Enable cluster mode for source Redis.")
	private boolean cluster;

	@Option(names = "--source-auto-reconnect", defaultValue = "true", fallbackValue = "true", negatable = true, description = "Automatically reconnect to source on connection loss. True by default.", hidden = true)
	private boolean autoReconnect = RedisClientOptions.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--source-resp", description = "Redis protocol version used to connect to source Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = RedisClientOptions.DEFAULT_PROTOCOL_VERSION;

	@ArgGroup(exclusive = false)
	private ReplicateRedisReaderArgs readerArgs = new ReplicateRedisReaderArgs();

	public RedisURI redisURI(RedisURI uri) {
		RedisURI.Builder builder = RedisURI.builder(uri);
		RedisArgs.configure(builder, username, password);
		RedisURI redisURI = builder.build();
		if (!StringUtils.hasLength(redisURI.getClientName())) {
			redisURI.setClientName(clientName);
		}
		return redisURI;
	}

	public RedisClientOptions redisClientOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setAutoReconnect(autoReconnect);
		options.setCluster(cluster);
		options.setProtocolVersion(protocolVersion);
		return options;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
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

	public ReplicateRedisReaderArgs getReaderArgs() {
		return readerArgs;
	}

	public void setReaderArgs(ReplicateRedisReaderArgs args) {
		this.readerArgs = args;
	}

}
