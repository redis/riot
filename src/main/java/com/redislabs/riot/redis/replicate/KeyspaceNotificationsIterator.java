package com.redislabs.riot.redis.replicate;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.cluster.pubsub.api.sync.RedisClusterPubSubCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyspaceNotificationsIterator implements KeyIterator {

	private StatefulRedisPubSubConnection<String, String> connection;
	private String channel;
	private int queueCapacity;

	private KeyspaceNotificationsListener listener = new KeyspaceNotificationsListener();
	private ClusterKeyspaceNotificationsListener clusterListener = new ClusterKeyspaceNotificationsListener();
	private BlockingQueue<String> queue;
	private boolean stopped;

	@Builder
	private KeyspaceNotificationsIterator(StatefulRedisPubSubConnection<String, String> connection, String channel,
			int queueCapacity) {
		this.connection = connection;
		this.channel = channel;
		this.queueCapacity = queueCapacity;
	}

	@Override
	public void start() {
		log.debug("Creating queue with capacity {}", queueCapacity);
		queue = new LinkedBlockingDeque<String>(queueCapacity);
		if (cluster()) {
			StatefulRedisClusterPubSubConnection<String, String> clusterConnection = (StatefulRedisClusterPubSubConnection<String, String>) connection;
			clusterConnection.addListener(clusterListener);
			clusterConnection.setNodeMessagePropagation(true);
			RedisClusterPubSubCommands<String, String> sync = clusterConnection.sync();
			sync.masters().commands().psubscribe(channel);
			log.debug("Subscribed to channel {}", channel);
		} else {
			connection.addListener(listener);
			connection.sync().psubscribe(channel);
		}
		stopped = false;
	}

	private class KeyspaceNotificationsListener extends RedisPubSubAdapter<String, String> {

		@Override
		public void message(String pattern, String channel, String message) {
			KeyspaceNotificationsIterator.this.message(channel);
		}

	}

	private class ClusterKeyspaceNotificationsListener extends RedisClusterPubSubAdapter<String, String> {

		@Override
		public void message(RedisClusterNode node, String pattern, String channel, String message) {
			KeyspaceNotificationsIterator.this.message(channel);
		}

	}

	private boolean cluster() {
		return connection instanceof StatefulRedisClusterPubSubConnection;
	}

	@Override
	public void stop() {
		stopped = true;
		if (cluster()) {
			StatefulRedisClusterPubSubConnection<String, String> clusterConnection = (StatefulRedisClusterPubSubConnection<String, String>) connection;
			clusterConnection.sync().masters().commands().punsubscribe(channel);
			clusterConnection.removeListener(clusterListener);
		} else {
			connection.sync().punsubscribe(channel);
			connection.removeListener(listener);
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

	public void message(String channel) {
		String key = channel.substring(channel.indexOf(":") + 1);
		try {
			queue.put(key);
		} catch (InterruptedException e) {
			// ignore
		}
	}

}
