package com.redis.riot;

import java.time.Duration;
import java.util.Arrays;

import com.redis.lettucemod.RedisURIBuilder;
import com.redis.riot.core.RiotVersion;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import picocli.CommandLine.Option;

public class RedisURIArgs {

	@Option(names = { "-u", "--uri" }, description = "Server URI.", paramLabel = "<uri>")
	private RedisURI uri;

	@Option(names = { "-h",
			"--host" }, description = "Server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = RedisURIBuilder.DEFAULT_HOST;

	@Option(names = { "-p", "--port" }, description = "Server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisURIBuilder.DEFAULT_PORT;

	@Option(names = { "-s",
			"--socket" }, description = "Server socket (overrides hostname and port).", paramLabel = "<socket>")
	private String socket;

	@Option(names = "--user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = { "-a",
			"--pass" }, arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<password>")
	private char[] password;

	@Option(names = "--timeout", description = "Redis command timeout in seconds (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long timeout = RedisURIBuilder.DEFAULT_TIMEOUT;

	@Option(names = { "-n", "--db" }, description = "Database number.", paramLabel = "<db>")
	private int database;

	@Option(names = "--tls", description = "Establish a secure TLS connection.")
	private boolean tls;

	@Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean insecure;

	@Option(names = "--client", description = "Client name used to connect to Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RiotVersion.riotVersion();

	public RedisURI redisURI() {
		RedisURIBuilder builder = new RedisURIBuilder();
		builder.clientName(clientName);
		builder.database(database);
		builder.host(host);
		builder.password(password);
		builder.port(port);
		builder.socket(socket);
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

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	@Override
	public String toString() {
		return "RedisURIArgs [uri=" + uri + ", host=" + host + ", port=" + port + ", socket=" + socket + ", username="
				+ username + ", password=" + Arrays.toString(password) + ", timeout=" + timeout + ", database="
				+ database + ", tls=" + tls + ", insecure=" + insecure + ", clientName=" + clientName + "]";
	}

}
