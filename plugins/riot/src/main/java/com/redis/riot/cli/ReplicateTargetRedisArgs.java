package com.redis.riot.cli;

import org.springframework.util.StringUtils;

import com.redis.riot.cli.RedisReaderArgs.ReadFromEnum;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RiotVersion;

import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ReplicateTargetRedisArgs {

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-client", description = "Client name used to connect to target Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RiotVersion.riotVersion();

	@Option(names = "--target-cluster", description = "Enable cluster mode for target redis.")
	private boolean cluster;

	@Option(names = "--target-auto-reconnect", defaultValue = "true", fallbackValue = "true", negatable = true, description = "Automatically reconnect to target on connection loss. True by default.", hidden = true)
	private boolean autoReconnect = true;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion;

	@Option(names = "--target-read-from", description = "Which target cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>", hidden = true)
	private ReadFromEnum readFrom;

	@ArgGroup(exclusive = false)
	private RedisWriterArgs writerArgs = new RedisWriterArgs();

	public RedisURI redisURI(RedisURI uri) {
		Builder builder = RedisURI.builder(uri);
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

	public ReadFromEnum getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFromEnum readFrom) {
		this.readFrom = readFrom;
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

	public RedisWriterArgs getWriterArgs() {
		return writerArgs;
	}

	public void setWriterArgs(RedisWriterArgs writerArgs) {
		this.writerArgs = writerArgs;
	}

}
