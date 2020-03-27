package com.redislabs.riot.redis.writer;

import java.util.List;

import lombok.Builder;
import lombok.Setter;
import redis.clients.jedis.JedisCluster;

public class JedisClusterWriter<O> extends AbstractRedisItemWriter<O> {

	private @Setter JedisCluster cluster;

	@Builder
	protected JedisClusterWriter(CommandWriter<O> writer, JedisCluster cluster) {
		super(writer);
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
