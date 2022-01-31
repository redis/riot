package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.processor.KeyValueKeyProcessor;
import com.redis.riot.processor.KeyValueTTLProcessor;
import com.redis.spring.batch.KeyValue;

import picocli.CommandLine.Option;

public class KeyValueProcessorOptions {

	@Option(names = "--key-process", description = "SpEL expression to transform each key", paramLabel = "<exp>")
	private String keyProcessor;
	@Option(names = "--ttl-process", description = "SpEL expression to transform each key TTL", paramLabel = "<exp>")
	private String ttlProcessor;

	public String getKeyProcessor() {
		return keyProcessor;
	}

	public String getTtlProcessor() {
		return ttlProcessor;
	}

	public <T extends KeyValue<byte[], ?>> ItemProcessor<T, T> processor(RedisOptions sourceRedis,
			RedisOptions targetRedis) {
		SpelExpressionParser parser = new SpelExpressionParser();
		List<ItemProcessor<T, T>> processors = new ArrayList<>();
		if (keyProcessor != null) {
			EvaluationContext context = new StandardEvaluationContext();
			context.setVariable("src", sourceRedis.uris().get(0));
			context.setVariable("dest", targetRedis.uris().get(0));
			processors.add(new KeyValueKeyProcessor<>(parser.parseExpression(keyProcessor, new TemplateParserContext()),
					context));
		}
		if (ttlProcessor != null) {
			processors.add(
					new KeyValueTTLProcessor<>(parser.parseExpression(ttlProcessor), new StandardEvaluationContext()));
		}
		return CompositeItemStreamItemProcessor.delegates(processors);
	}

}
