package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class SuggestionIndexWriter extends AbstractIndexWriter {

	private RediSearchClient client;
	private RediSearchCommands<String, String> commands;

	public SuggestionIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index,
			RediSearchClient client) {
		super(template, entity, index);
		this.client = client;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = client.connect().sync();
	}

	@Override
	protected void writeIndex(StringRedisConnection conn, String key, String id, Map<String, Object> record) {
		String string = convert(record.get(getConfig().getSuggestion()), String.class);
		Double score = getConfig().getScore() == null ? 1d : convert(record.get(getConfig().getScore()), Double.class);
		commands.suggestionAdd(getConfig().getName(), string, score);
	}

}
