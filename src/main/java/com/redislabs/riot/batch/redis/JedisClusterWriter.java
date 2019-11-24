package com.redislabs.riot.batch.redis;

import java.util.List;

import lombok.experimental.Accessors;
import redis.clients.jedis.JedisCluster;

@Accessors(fluent = true)
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
