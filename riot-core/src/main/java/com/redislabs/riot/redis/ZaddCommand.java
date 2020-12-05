package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.SortedSetItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zadd")
public class ZaddCommand extends AbstractCollectionCommand {

	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault = 1;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return configure(SortedSetItemWriter.<Map<String, Object>>builder().client(client).poolConfig(poolConfig)
				.scoreConverter(numberFieldExtractor(Double.class, scoreField, scoreDefault))).build();
	}

}
