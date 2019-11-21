package com.redislabs.riot.batch.redis;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;

@Slf4j
public class JedisClusterWriter<O> extends AbstractRedisItemWriter<O> {

	private JedisCluster cluster;
	private RedisWriter<JedisCluster, O> writer;

	public JedisClusterWriter(JedisCluster cluster, RedisWriter<JedisCluster, O> writer) {
		this.cluster = cluster;
		this.writer = writer;
	}

	@Override
	public void write(List<? extends O> items) {
		for (O item : items) {
			try {
				writer.write(cluster, item);
			} catch (Exception e) {
				log.error("Could not write item {}", item, e);
			}
		}
	}

	@Override
	public void close() {
		cluster.close();
		super.close();
	}
}
