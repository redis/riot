package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.ruaux.pojofaker.Faker;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

public class GeneratorEntityItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private String locale;
	private SpelExpressionParser parser = new SpelExpressionParser();
	private EvaluationContext context;
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private RediSearchClient client;
	private StatefulRediSearchConnection<String, String> connection;

	public GeneratorEntityItemReader(RediSearchClient client, String locale, Map<String, String> fields) {
		this.client = client;
		this.locale = locale;
		fields.forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
	}

	@Override
	protected void doOpen() throws Exception {
		connection = client.connect();
		context = new StandardEvaluationContext(new Faker(new Locale(locale)));
		context.setVariable("redis", connection.sync());
		context.setVariable("sequence", new RedisSequence(connection));
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		expressions.forEach((k, v) -> map.put(k, v.getValue(context)));
		return map;
	}

	@Override
	protected synchronized void doClose() throws Exception {
		if (connection != null) {
			connection.close();
			connection = null;
		}
		context = null;
	}

}
