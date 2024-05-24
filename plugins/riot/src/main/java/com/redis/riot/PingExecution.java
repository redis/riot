package com.redis.riot;

import java.time.Duration;

public class PingExecution {

	private static final String PONG = "pong";

	private final long startTime = time();
	private String reply;
	private long endTime;

	public PingExecution reply(String reply) {
		this.reply = reply;
		this.endTime = time();
		return this;
	}

	public boolean isSuccess() {
		return PONG.equalsIgnoreCase(reply);
	}

	public String getReply() {
		return reply;
	}

	public Duration getDuration() {
		return Duration.ofNanos(endTime - startTime);
	}

	private static long time() {
		return System.nanoTime();
	}

}