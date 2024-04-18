package com.redis.riot.cli;

import java.time.Duration;

import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import picocli.CommandLine.Option;

public class RedisUriArgs {

	@Option(names = { "-u", "--uri" }, description = "Redis server URI.", paramLabel = "<uri>")
	private String uri;

	@Option(names = { "-h",
			"--host" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = "localhost";

	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisURI.DEFAULT_REDIS_PORT;

	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private String socket;

	@Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private char[] password;

	@Option(names = "--timeout", description = "Redis command timeout in seconds.", paramLabel = "<sec>")
	private long timeout;

	@Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
	private int database;

	@Option(names = "--client", description = "Client name used to connect to Redis.", paramLabel = "<name>")
	private String clientName;

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean insecure;

	@Option(names = { "-c", "--cluster" }, description = "Enable cluster mode.")
	private boolean cluster;

	public RedisURI redisURI() {
		RedisURI.Builder builder = redisURIBuilder();
		if (database > 0) {
			builder.withDatabase(database);
		}
		if (StringUtils.hasLength(clientName)) {
			builder.withClientName(clientName);
		}
		if (!ObjectUtils.isEmpty(password)) {
			if (StringUtils.hasLength(username)) {
				builder.withAuthentication(username, password);
			} else {
				builder.withPassword(password);
			}
		}
		if (tls) {
			builder.withSsl(tls);
			if (insecure) {
				builder.withVerifyPeer(SslVerifyMode.NONE);
			}
		}
		if (timeout > 0) {
			builder.withTimeout(Duration.ofSeconds(timeout));
		}
		return builder.build();
	}

	private RedisURI.Builder redisURIBuilder() {
		if (StringUtils.hasLength(uri)) {
			return RedisURI.builder(RedisURI.create(uri));
		}
		if (StringUtils.hasLength(socket)) {
			return RedisURI.Builder.socket(socket);
		}
		return RedisURI.Builder.redis(host, port);
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
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

}
