package com.redislabs.riot.batch.redis.writer;

import java.util.List;

import redis.clients.jedis.JedisCluster;

public class JedisClusterWriter<O> extends AbstractRedisItemWriter<JedisCluster, O> {

	private JedisCluster cluster;

	public JedisClusterWriter(JedisCluster cluster) {
		this.cluster = cluster;
	}

	@Override
	public void write(List<? extends O> items) {
		for (O item : items) {
			try {
				writer.write(cluster, item);
			} catch (Exception e) {
				logWriteError(item, e);
			}
		}
	}

	@Override
	public void close() {
		cluster.close();
		super.close();
	}
}
