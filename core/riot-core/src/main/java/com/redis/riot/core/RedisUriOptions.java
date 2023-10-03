package com.redis.riot.core;

import java.time.Duration;

import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;

public class RedisUriOptions {

    public static final String DEFAULT_HOST = "127.0.0.1";

    public static final int DEFAULT_PORT = 6379;

    public static final SslVerifyMode DEFAULT_VERIFY_PEER = SslVerifyMode.FULL;

    private String uri;

    private String host = DEFAULT_HOST;

    private int port = DEFAULT_PORT;

    private String socket;

    private String username;

    private char[] password;

    private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    private int database;

    private boolean tls;

    private SslVerifyMode verifyPeer = DEFAULT_VERIFY_PEER;

    private String clientName;

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
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

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
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

    public SslVerifyMode getVerifyPeer() {
        return verifyPeer;
    }

    public void setVerifyPeer(SslVerifyMode mode) {
        this.verifyPeer = mode;
    }

}
