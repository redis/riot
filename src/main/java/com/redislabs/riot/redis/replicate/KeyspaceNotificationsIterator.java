package com.redislabs.riot.redis.replicate;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyspaceNotificationsIterator extends RedisPubSubAdapter<String, String> implements KeyIterator {

	@Getter
	@Setter
	private StatefulRedisPubSubConnection<String, String> pubSubConnection;
	@Getter
	@Setter
	private String channel;
	@Getter
	@Setter
	private int queueCapacity;

	private BlockingQueue<String> queue;
	private boolean stopped;

	public KeyspaceNotificationsIterator(StatefulRedisPubSubConnection<String, String> pubSubConnection, String channel,
			int queueCapacity) {
		this.pubSubConnection = pubSubConnection;
		this.channel = channel;
		this.queueCapacity = queueCapacity;
	}

	@Override
	public void start() {
		log.debug("Creating queue with capacity {}", queueCapacity);
		queue = new LinkedBlockingDeque<String>(queueCapacity);
		pubSubConnection.addListener(this);
		pubSubConnection.sync().psubscribe(channel);
		stopped = false;
	}

	@Override
	public void stop() {
		stopped = true;
		pubSubConnection.sync().punsubscribe(channel);
		pubSubConnection.removeListener(this);
	}

	@Override
	public void message(String pattern, String channel, String message) {
		String key = channel.substring(channel.indexOf(":") + 1);
		try {
			queue.put(key);
		} catch (InterruptedException e) {
			// ignore
		}
	}

	@Override
	public boolean hasNext() {
		return !stopped;
	}

	@Override
	public String next() {
		try {
			String key;
			do {
				key = queue.poll(100, TimeUnit.MILLISECONDS);
			} while (key == null && !stopped);
			return key;
		} catch (InterruptedException e) {
			return null;
		}
	}

}
