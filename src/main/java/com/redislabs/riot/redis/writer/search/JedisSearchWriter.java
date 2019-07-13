package com.redislabs.riot.redis.writer.search;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.redis.RedisConverter;

import io.redisearch.Document;
import io.redisearch.client.AddOptions;
import io.redisearch.client.Client;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JedisSearchWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private Client client;
	private RedisConverter converter;
	private AddOptions options;
	private String scoreField;
	private String payloadField;
	private float defaultScore;

	public JedisSearchWriter(Client client, RedisConverter converter, AddOptions options) {
		setName(ClassUtils.getShortName(JedisSearchWriter.class));
		this.client = client;
		this.converter = converter;
		this.options = options;
	}

	public void setScoreField(String scoreField) {
		this.scoreField = scoreField;
	}

	public void setPayloadField(String payloadField) {
		this.payloadField = payloadField;
	}

	public void setDefaultScore(float defaultScore) {
		this.defaultScore = defaultScore;
	}

	public void write(List<? extends Map<String, Object>> items) throws Exception {
		Document[] docs = items.stream().map(item -> document(item)).toArray(Document[]::new);
		boolean[] result = client.addDocuments(options, docs);
		for (int index = 0; index < result.length; index++) {
			if (!result[index]) {
				log.error("Could not add document {}", items.get(index));
			}
		}
	}

	private Document document(Map<String, Object> item) {
		Float score = converter.convert(item.getOrDefault(scoreField, defaultScore), Float.class);
		byte[] payload = payload(item);
		return new Document(converter.key(item), item, score, payload);
	}

	private byte[] payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		String payload = converter.convert(item.get(payloadField), String.class);
		if (payload == null) {
			return null;
		}
		return payload.getBytes();
	}

}
