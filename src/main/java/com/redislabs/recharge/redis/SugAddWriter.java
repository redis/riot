package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.SuggestConfiguration;

import io.redisearch.Suggestion;
import io.redisearch.client.Client;

public class SugAddWriter extends AbstractRedisWriter {

	private Client client;
	private SuggestConfiguration suggest;

	public SugAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer, Client client) {
		super(template, writer);
		this.suggest = writer.getSuggest();
		this.client = client;
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		String string = convert(record.get(suggest.getField()), String.class);
		double score = suggest.getScore() == null ? suggest.getDefaultScore()
				: convert(record.get(suggest.getScore()), Double.class);
		Suggestion suggestion = Suggestion.builder().str(string).score(score).build();
		client.addSuggestion(suggestion, suggest.isIncrement());
	}

}
