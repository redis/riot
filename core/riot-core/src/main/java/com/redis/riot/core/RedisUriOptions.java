package com.redis.riot.core;

import java.time.Duration;

import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;

public class RedisUriOptions {

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = RedisURI.DEFAULT_REDIS_PORT;

	private String uri;
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private String socket;
	private String username;
	private char[] password;
	private long timeout;
	private int database;
	private String clientName = RiotVersion.riotVersion();
	private boolean tls;
	private boolean insecure;

	public RedisURI redisURI() {
		RedisURI.Builder builder = redisURIBuilder();
		if (!ObjectUtils.isEmpty(password)) {
			if (StringUtils.hasLength(username)) {
				builder.withAuthentication(username, password);
			} else {
				builder.withPassword(password);
			}
		}
		if (StringUtils.hasLength(clientName)) {
			builder.withClientName(clientName);
		}
		if (database > 0) {
			builder.withDatabase(database);
		}
		if (tls) {
			builder.withSsl(tls);
		}
		if (insecure) {
			builder.withVerifyPeer(SslVerifyMode.NONE);
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

}
