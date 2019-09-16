package com.redislabs.riot.redis;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.writer.JedisItemWriter;

import redis.clients.jedis.JedisCluster;

public class JedisClusterWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(JedisClusterWriter.class);

	private JedisCluster cluster;
	private JedisItemWriter writer;

	public JedisClusterWriter(JedisCluster cluster, JedisItemWriter writer) {
		setName(ClassUtils.getShortName(JedisClusterWriter.class));
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
