package com.redis.riot;

import java.time.Duration;

import com.redis.lettucemod.RedisURIBuilder;
import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.item.redis.RedisItemReader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class SourceRedisArgs {

	@Option(names = "--source-user", description = "Source ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--source-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the source server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--source-timeout", description = "Source Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = RedisURIBuilder.DEFAULT_TIMEOUT;

	@Option(names = "--source-tls", description = "Establish a secure TLS connection to source.")
	private boolean tls;

	@Option(names = "--source-insecure", description = "Allow insecure TLS connection to source by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--source-client", description = "Client name used to connect to source Redis.", paramLabel = "<name>")
	private String clientName;

	@Option(names = "--source-cluster", description = "Enable source cluster mode.")
	private boolean cluster;

	@Option(names = "--source-resp", description = "Redis protocol version used to connect to source: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = ProtocolVersion.RESP2;

	@Option(names = "--source-pool", description = "Max pool connections used for source Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisItemReader.DEFAULT_POOL_SIZE;

	@Option(names = "--source-command-metrics", description = "Enable Lettuce command metrics for source Redis", hidden = true)
	private boolean metrics;

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

	public boolean isMetrics() {
		return metrics;
	}

	public void setMetrics(boolean metrics) {
		this.metrics = metrics;
	}

	@Override
	public String toString() {
		return "SourceRedisArgs [username=" + username + ", password=" + RiotUtils.mask(password) + ", timeout="
				+ timeout + ", tls=" + tls + ", insecure=" + insecure + ", clientName=" + clientName + ", cluster="
				+ cluster + ", protocolVersion=" + protocolVersion + ", poolSize=" + poolSize + ", metrics=" + metrics
				+ "]";
	}

	public RedisContext redisContext(RedisURI uri, SslArgs sslArgs) {
		return RedisContext.create(redisURI(uri), cluster, protocolVersion, sslArgs, metrics);
	}

}
