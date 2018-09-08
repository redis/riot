package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.SuggestConfiguration;

public class SugAddWriter extends AbstractRedisWriter {

	private RediSearchClient client;
	private RediSearchCommands<String, String> commands;
	private SuggestConfiguration suggest;

	public SugAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer, RediSearchClient client) {
		super(template, writer);
		this.suggest = writer.getSuggest();
		this.client = client;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = client.connect().sync();
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		String string = convert(record.get(suggest.getField()), String.class);
		Double score = suggest.getScore() == null ? suggest.getDefaultScore()
				: convert(record.get(suggest.getScore()), Double.class);
		commands.suggestionAdd(key, string, score);
	}

}
