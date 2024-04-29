package com.redis.riot.cli;

import com.redis.riot.core.RedisUriOptions;
import com.redis.riot.core.RiotVersion;

import picocli.CommandLine.Option;

public class ReplicateTargetRedisUriArgs {

	@Option(names = "--target-uri", description = "Target server URI.", paramLabel = "<uri>")
	private String uri;

	@Option(names = "--target-host", description = "Target server hostname (default: ${DEFAULT-VALUE}).", paramLabel = "<host>")
	private String host = RedisUriOptions.DEFAULT_HOST;

	@Option(names = "--target-port", description = "Target server port (default: ${DEFAULT-VALUE}).", paramLabel = "<port>")
	private int port = RedisUriOptions.DEFAULT_PORT;

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-client", description = "Client name used to connect to target Redis (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private String clientName = RiotVersion.riotVersion();

	public RedisUriOptions redisUriOptions() {
		RedisUriOptions options = new RedisUriOptions();
		options.setClientName(clientName);
		options.setHost(host);
		options.setPort(port);
		options.setPassword(password);
		options.setUri(uri);
		options.setUsername(username);
		return options;
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

}
