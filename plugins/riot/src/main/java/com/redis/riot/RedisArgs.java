package com.redis.riot;

import java.time.Duration;
import java.util.Arrays;

import com.redis.riot.RedisClientBuilder.RedisURIClient;
import com.redis.spring.batch.item.redis.common.OperationExecutor;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisArgs {

	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;

	@Option(names = { "-u", "--uri" }, description = "Server URI.", paramLabel = "<uri>")
	private RedisURI uri;

	@Option(names = { "-h",
			"--host" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = RedisClientBuilder.DEFAULT_HOST;

	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisClientBuilder.DEFAULT_PORT;

	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private String socket;

	@Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private char[] password;

	@Option(names = "--timeout", description = "Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = RedisClientBuilder.DEFAULT_TIMEOUT;

	@Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
	private int database;

	@Option(names = "--client", description = "Client name used to connect to Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RedisClientBuilder.DEFAULT_CLIENT_NAME;

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean insecure;

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	@Option(names = "--auto-reconnect", description = "Automatically reconnect on connection loss. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true", hidden = true)
	private boolean autoReconnect = RedisClientBuilder.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES}.", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = RedisClientBuilder.DEFAULT_PROTOCOL_VERSION;

	@ArgGroup(exclusive = false)
	private SslArgs sslArgs = new SslArgs();

	@Option(names = "--pool", description = "Max connections for Redis pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = DEFAULT_POOL_SIZE;

	public RedisURIClient redisURIClient() {
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

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
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

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs sslArgs) {
		this.sslArgs = sslArgs;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public String toString() {
		return "RedisArgs [uri=" + uri + ", host=" + host + ", port=" + port + ", socket=" + socket + ", username="
				+ username + ", password=" + Arrays.toString(password) + ", timeout=" + timeout + ", database="
				+ database + ", clientName=" + clientName + ", tls=" + tls + ", insecure=" + insecure + ", cluster="
				+ cluster + ", autoReconnect=" + autoReconnect + ", protocolVersion=" + protocolVersion + ", sslArgs="
				+ sslArgs + ", poolSize=" + poolSize + "]";
	}

}
