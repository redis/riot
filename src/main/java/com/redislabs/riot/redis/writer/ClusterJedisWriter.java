package com.redislabs.riot.redis.writer;

import java.util.List;

import redis.clients.jedis.JedisCluster;

public class ClusterJedisWriter<O> extends AbstractRedisItemWriter<O> {

	private JedisCluster cluster;

	public ClusterJedisWriter(JedisCluster cluster) {
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

}
