package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class SuggestionIndexWriter extends AbstractIndexWriter {

	private RediSearchClient client;
	private RediSearchCommands<String, String> commands;
	private String indexName;
	private ConversionService converter = new DefaultConversionService();
	private String suggestionField;
	private String scoreField;

	public SuggestionIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			Entry<String, IndexConfiguration> index, RediSearchClient client) {
		super(template, entity, index);
		this.indexName = index.getKey();
		this.suggestionField = index.getValue().getSuggestion();
		this.scoreField = index.getValue().getScore();
		this.client = client;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = client.connect().sync();
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key,
			String indexKey) {
		String string = converter.convert(record.get(suggestionField), String.class);
		Double score = scoreField == null ? 1d : converter.convert(record.get(scoreField), Double.class);
		commands.suggestionAdd(indexName, string, score);
	}

	@Override
	protected String getDefaultKeyspace() {
		return "search";
	}

}
