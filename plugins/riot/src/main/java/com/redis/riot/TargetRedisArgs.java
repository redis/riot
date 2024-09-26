package com.redis.riot;

import java.time.Duration;

import com.redis.lettucemod.RedisURIBuilder;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.RiotVersion;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class TargetRedisArgs {

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-timeout", description = "Target Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = RedisURIBuilder.DEFAULT_TIMEOUT;

	@Option(names = "--target-tls", description = "Establish a secure TLS connection to target.")
	private boolean tls;

	@Option(names = "--target-insecure", description = "Allow insecure TLS connection to target by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--target-client", description = "Client name used to connect to target Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RiotVersion.riotVersion();

	@Option(names = "--target-cluster", description = "Enable target cluster mode.")
	private boolean cluster;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = ProtocolVersion.RESP2;

	@Option(names = "--target-pool", description = "Max pool connections used for target Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisItemWriter.DEFAULT_POOL_SIZE;

	public RedisURI redisURI(RedisURI uri) {
		RedisURIBuilder builder = new RedisURIBuilder();
		builder.uri(uri);
		builder.clientName(clientName);
		builder.password(password);
		builder.timeout(Duration.ofSeconds(timeout));
		builder.tls(tls);
		builder.username(username);
		if (insecure) {
			builder.verifyMode(SslVerifyMode.NONE);
		}
		return builder.build();
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

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	@Override
	public String toString() {
		return "TargetRedisArgs [username=" + username + ", password=" + RiotUtils.mask(password) + ", timeout="
				+ timeout + ", tls=" + tls + ", insecure=" + insecure + ", clientName=" + clientName + ", cluster="
				+ cluster + ", protocolVersion=" + protocolVersion + ", poolSize=" + poolSize + "]";
	}

}
