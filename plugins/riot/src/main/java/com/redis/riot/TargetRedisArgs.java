package com.redis.riot;

import java.util.Arrays;

import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class TargetRedisArgs {

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-insecure", description = "Allow insecure TLS connection to target by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--target-cluster", description = "Enable cluster mode for target.")
	private boolean cluster;

	@Option(names = "--target-auto-reconnect", description = "Automatically reconnect to target on connection loss. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean autoReconnect = RedisClientBuilder.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion;

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

	public boolean isInsecure() {
		return insecure;
	}

	public void setInsecure(boolean insecure) {
		this.insecure = insecure;
	}

	@Override
	public String toString() {
		return "TargetRedisArgs [username=" + username + ", password=" + Arrays.toString(password) + ", insecure="
				+ insecure + ", cluster=" + cluster + ", autoReconnect=" + autoReconnect + ", protocolVersion="
				+ protocolVersion + "]";
	}

	public RedisClientBuilder configure(RedisClientBuilder builder) {
		builder.autoReconnect(autoReconnect);
		builder.cluster(cluster);
		builder.password(password);
		builder.protocolVersion(protocolVersion);
		builder.username(username);
		if (insecure) {
			builder.verifyMode(SslVerifyMode.NONE);
		}
		return builder;

		
	}

}
