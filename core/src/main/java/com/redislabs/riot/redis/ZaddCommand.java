package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.SortedSetItemWriter;

import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "zadd", description = "Add members with scores sorted sets")
public class ZaddCommand extends AbstractCollectionCommand {

	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault = 1;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return configure(
				SortedSetItemWriter.<Map<String, Object>>builder().client(client).poolConfig(redisOptions.poolConfig())
						.scoreConverter(numberFieldExtractor(Double.class, scoreField, scoreDefault))).build();
	}

}
