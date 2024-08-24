package com.redis.riot;

import com.redis.riot.core.RiotUtils;

import picocli.CommandLine.Option;

public class SourceRedisURIArgs {

	@Option(names = "--source-user", description = "ACL style 'AUTH username pass'. Needs password.", paramLabel = "<name>")
	private String username;

	@Option(names = "--source-pass", arity = "0..1", interactive = true, description = "Password to use when connecting to the server.", paramLabel = "<pwd>")
	private char[] password;

	@Option(names = "--source-insecure", description = "Allow insecure TLS connection by skipping cert validation.")
	private boolean insecure;

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
		return "SourceRedisURIArgs [username=" + username + ", password=" + RiotUtils.mask(password) + ", insecure="
				+ insecure + "]";
	}

}
