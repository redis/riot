package com.redis.riot.core;

import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Resource;

public class RedisSslOptions {

	private Resource keystore;
	private char[] keystorePassword;
	private Resource truststore;
	private char[] truststorePassword;
	private Resource keyCert;
	private Resource key;
	private char[] keyPassword;
	private Resource trustedCerts;

	public Resource getKeystore() {
		return keystore;
	}

	public void setKeystore(Resource keystore) {
		this.keystore = keystore;
	}

	public char[] getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(char[] keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public Resource getTruststore() {
		return truststore;
	}

	public void setTruststore(Resource truststore) {
		this.truststore = truststore;
	}

	public char[] getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(char[] truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public Resource getKeyCert() {
		return keyCert;
	}

	public void setKeyCert(Resource keyCert) {
		this.keyCert = keyCert;
	}

	public Resource getKey() {
		return key;
	}

	public void setKey(Resource key) {
		this.key = key;
	}

	public char[] getKeyPassword() {
		return keyPassword;
	}

	public void setKeyPassword(char[] keyPassword) {
		this.keyPassword = keyPassword;
	}

	public Resource getTrustedCerts() {
		return trustedCerts;
	}

	public void setTrustedCerts(Resource trustedCerts) {
		this.trustedCerts = trustedCerts;
	}

	public SslOptions sslOptions() {
		SslOptions.Builder ssl = SslOptions.builder();
		if (key != null) {
			ssl.keyManager(keyCert, key, keyPassword);
		}
		if (trustedCerts != null) {
			ssl.trustManager(trustedCerts);
		}
		if (keystore != null) {
			ssl.keystore(keystore, keystorePassword);
		}
		if (truststore != null) {
			ssl.truststore(truststore, truststorePassword);
		}
		return ssl.build();
	}

}
