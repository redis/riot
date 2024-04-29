package com.redis.riot.cli;

import java.io.File;

import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.SslOptions.Resource;
import picocli.CommandLine.Option;

public class SslArgs {

	@Option(names = "--keystore", description = "Path to keystore.", paramLabel = "<file>", hidden = true)
	private File keystore;

	@Option(names = "--keystore-pass", arity = "0..1", interactive = true, description = "Keystore password.", paramLabel = "<password>", hidden = true)
	private char[] keystorePassword;

	@Option(names = "--trust", description = "Path to truststore.", paramLabel = "<file>", hidden = true)
	private File truststore;

	@Option(names = "--trust-pass", arity = "0..1", interactive = true, description = "Truststore password.", paramLabel = "<password>", hidden = true)
	private char[] truststorePassword;

	@Option(names = "--cert", description = "Client certificate to authenticate with (X.509 PEM).", paramLabel = "<file>")
	private File keyCert;

	@Option(names = "--key", description = "Private key file to authenticate with (PKCS#8 PEM).", paramLabel = "<file>")
	private File key;

	@Option(names = "--key-pass", arity = "0..1", interactive = true, description = "Private key password.", paramLabel = "<pwd>")
	private char[] keyPassword;

	@Option(names = "--cacert", description = "CA Certificate file to verify with (X.509).", paramLabel = "<file>")
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
