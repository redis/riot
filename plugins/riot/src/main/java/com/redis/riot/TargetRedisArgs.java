package com.redis.riot;

import java.time.Duration;
import java.util.Arrays;

import com.redis.riot.RedisClientBuilder.RedisURIClient;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.Option;

public class TargetRedisArgs {

	@Option(names = "--target-uri", description = "Target server URI.", paramLabel = "<uri>")
	private RedisURI uri;

	@Option(names = "--target-host", description = "Target server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = RedisClientBuilder.DEFAULT_HOST;

	@Option(names = "--target-port", description = "Target server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisClientBuilder.DEFAULT_PORT;

	@Option(names = "--target-socket", description = "Target server socket (overrides hostname and port).", paramLabel = "<socket>", hidden = true)
	private String socket;

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-timeout", description = "Target Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = RedisClientBuilder.DEFAULT_TIMEOUT;

	@Option(names = "--target-db", description = "Database number.", paramLabel = "<db>", hidden = true)
	private int database;

	@Option(names = "--target-client", description = "Client name used to connect to target (default: ${DEFAULT-VALUE}).", paramLabel = "<name>", hidden = true)
	private String clientName = RedisClientBuilder.DEFAULT_CLIENT_NAME;

	@Option(names = "--target-tls", description = "Establish a secure TLS connection to target.")
	private boolean tls;

	@Option(names = "--target-insecure", description = "Allow insecure TLS connection to target by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--target-cluster", description = "Enable cluster mode for target.")
	private boolean cluster;

	@Option(names = "--target-auto-reconnect", description = "Automatically reconnect to target on connection loss. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean autoReconnect = RedisClientBuilder.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--target-resp", description = "Redis protocol version used to connect to target: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion;

	@Option(names = "--target-pool", description = "Max connections for target Redis pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisArgs.DEFAULT_POOL_SIZE;

	public RedisURIClient redisURIClient(SslArgs sslArgs) {
		RedisClientBuilder builder = new RedisClientBuilder();
		builder.autoReconnect(autoReconnect);
		builder.clientName(clientName);
		builder.cluster(cluster);
		builder.database(database);
		builder.host(host);
		builder.password(password);
		builder.port(port);
		builder.protocolVersion(protocolVersion);
		builder.socket(socket);
		builder.sslOptions(sslArgs.sslOptions());
		builder.timeout(Duration.ofSeconds(timeout));
		builder.tls(tls);
		builder.uri(uri);
		builder.username(username);
		if (insecure) {
			builder.verifyMode(SslVerifyMode.NONE);
		}
		return builder.build();
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

	public RedisURI getUri() {
		return uri;
	}

	public void setUri(RedisURI uri) {
		this.uri = uri;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getSocket() {
		return socket;
	}

	public void setSocket(String socket) {
		this.socket = socket;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	public boolean isInsecure() {
		return insecure;
	}

	public void setInsecure(boolean insecure) {
		this.insecure = insecure;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public String toString() {
		return "TargetRedisArgs [uri=" + uri + ", host=" + host + ", port=" + port + ", socket=" + socket
				+ ", username=" + username + ", password=" + Arrays.toString(password) + ", timeout=" + timeout
				+ ", database=" + database + ", clientName=" + clientName + ", tls=" + tls + ", insecure=" + insecure
				+ ", cluster=" + cluster + ", autoReconnect=" + autoReconnect + ", protocolVersion=" + protocolVersion
				+ ", poolSize=" + poolSize + "]";
	}

}
