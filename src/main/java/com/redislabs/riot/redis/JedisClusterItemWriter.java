package com.redislabs.riot.redis;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.RedisMapWriter;

import redis.clients.jedis.JedisCluster;

public class JedisClusterItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(JedisClusterItemWriter.class);

	private JedisCluster cluster;
	private RedisMapWriter writer;

	public JedisClusterItemWriter(JedisCluster cluster, RedisMapWriter writer) {
		setName(ClassUtils.getShortName(JedisClusterItemWriter.class));
		this.cluster = cluster;
		this.writer = writer;
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		for (Map<String, Object> item : items) {
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
