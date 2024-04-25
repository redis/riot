package com.redis.riot.cli;

import java.io.File;

import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.SslOptions.Resource;
import picocli.CommandLine.Option;

public class SslArgs {

	@Option(names = "--ks", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
	private File keystore;

	@Option(names = "--ks-pwd", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<pwd>", hidden = true)
	private char[] keystorePassword;

	@Option(names = "--ts", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
	private File truststore;

	@Option(names = "--ts-pwd", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<pwd>", hidden = true)
	private char[] truststorePassword;

	@Option(names = "--cert", description = "X.509 cert chain file to authenticate (PEM).", paramLabel = "<file>")
	private File keyCert;

	@Option(names = "--key", description = "PKCS#8 private key file to authenticate (PEM).", paramLabel = "<file>")
	private File key;

	@Option(names = "--key-pwd", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "<pwd>")
	private char[] keyPassword;

	@Option(names = "--cacert", description = "X.509 CA certificate file to verify with.", paramLabel = "<file>")
	private File trustedCerts;

	public SslOptions sslOptions() {
		Builder ssl = SslOptions.builder();
		if (key != null) {
			ssl.keyManager(keyCert, key, keyPassword);
		}
		if (keystore != null) {
			ssl.keystore(keystore, keystorePassword);
		}
		if (truststore != null) {
			ssl.truststore(Resource.from(truststore), truststorePassword);
		}
		if (trustedCerts != null) {
			ssl.trustManager(trustedCerts);
		}
		return ssl.build();
	}

	public File getKeystore() {
		return keystore;
	}

	public void setKeystore(File keystore) {
		this.keystore = keystore;
	}

	public char[] getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(char[] keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public File getTruststore() {
		return truststore;
	}

	public void setTruststore(File truststore) {
		this.truststore = truststore;
	}

	public char[] getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(char[] truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public File getKeyCert() {
		return keyCert;
	}

	public void setKeyCert(File keyCert) {
		this.keyCert = keyCert;
	}

	public File getKey() {
		return key;
	}

	public void setKey(File key) {
		this.key = key;
	}

	public char[] getKeyPassword() {
		return keyPassword;
	}

	public void setKeyPassword(char[] keyPassword) {
		this.keyPassword = keyPassword;
	}

	public File getTrustedCerts() {
		return trustedCerts;
	}

	public void setTrustedCerts(File trustedCerts) {
		this.trustedCerts = trustedCerts;
	}

}
