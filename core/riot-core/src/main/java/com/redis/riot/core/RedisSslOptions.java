package com.redis.riot.core;

import java.io.File;

import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.SslOptions.Resource;

public class RedisSslOptions {

    private File keystore;

    private char[] keystorePassword;

    private File truststore;

    private char[] truststorePassword;

    private File keyCert;

    private File key;

    private char[] keyPassword;

    private File trustedCerts;

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

}
