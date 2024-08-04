package com.redis.riot;

import java.util.Arrays;

import com.redis.lettucemod.RedisURIBuilder;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import picocli.CommandLine.Option;

public class TargetRedisURIArgs {

	@Option(names = "--target-user", description = "Target ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--target-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the target server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--target-insecure", description = "Allow insecure TLS connection to target by skipping cert validation.")
	private boolean insecure;

	public RedisURI redisURI(RedisURI uri) {
		RedisURIBuilder builder = new RedisURIBuilder();
		builder.password(password);
		builder.uri(uri);
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

	@Override
	public String toString() {
		return "TargetRedisURIArgs [username=" + username + ", password=" + Arrays.toString(password) + ", insecure="
				+ insecure + "]";
	}

}
