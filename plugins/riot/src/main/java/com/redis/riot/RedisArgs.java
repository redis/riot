package com.redis.riot;

import java.time.Duration;

import io.lettuce.core.RedisURI;
import io.lettuce.core.protocol.ProtocolVersion;
import lombok.ToString;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@ToString(exclude = "password")
public class RedisArgs implements RedisClientArgs {

	@Option(names = { "-u", "--uri" }, description = "Redis server URI.", paramLabel = "<uri>")
	private RedisURI uri;

	@Option(names = { "-h",
			"--host" }, description = "Redis server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = DEFAULT_HOST;

	@Option(names = { "-p",
			"--port" }, description = "Redis server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = DEFAULT_PORT;

	@Option(names = { "-s",
			"--socket" }, description = "Redis server socket (overrides hostname and port).", paramLabel = "<socket>")
	private String socket;

	@Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the Redis server.", paramLabel = "<password>")
	private char[] password;

	@Option(names = "--timeout", description = "Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = DEFAULT_TIMEOUT_SECONDS;

	@Option(names = { "-n", "--db" }, description = "Redis database number.", paramLabel = "<db>")
	private int database = DEFAULT_DATABASE;

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private String clientName;

	@Option(names = { "-c", "--cluster" }, description = "Enable Redis cluster mode.")
	private boolean cluster;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = DEFAULT_PROTOCOL_VERSION;

	@ArgGroup(exclusive = false)
	private SslArgs sslArgs = new SslArgs();

	@Option(names = "--pool", description = "Max number of Redis connections (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = DEFAULT_POOL_SIZE;

	@Option(names = "--read-from", description = "Which Redis cluster nodes to read from: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private ReadFrom readFrom = DEFAULT_READ_FROM;

	@Override
	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	@Override
	public ProtocolVersion getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(ProtocolVersion version) {
		this.protocolVersion = version;
	}

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs sslArgs) {
		this.sslArgs = sslArgs;
	}

	public RedisURI getUri() {
		return uri;
	}

	public void setUri(RedisURI uri) {
		this.uri = uri;
	}

	@Override
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String getSocket() {
		return socket;
	}

	public void setSocket(String socket) {
		this.socket = socket;
	}

	@Override
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@Override
	public char[] getPassword() {
		return password;
	}

	public void setPassword(char[] password) {
		this.password = password;
	}

	@Override
	public Duration getTimeout() {
		return Duration.ofSeconds(timeout);
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	@Override
	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	@Override
	public boolean isTls() {
		return tls;
	}

	public void setTls(boolean tls) {
		this.tls = tls;
	}

	@Override
	public boolean isInsecure() {
		return insecure;
	}

	public void setInsecure(boolean insecure) {
		this.insecure = insecure;
	}

	@Override
	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	@Override
	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	@Override
	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

}
