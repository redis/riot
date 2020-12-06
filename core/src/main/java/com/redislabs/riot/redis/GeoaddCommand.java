package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.GeoItemWriter;

import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description = "Add geospatial items")
public class GeoaddCommand extends AbstractCollectionCommand {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client, RedisOptions redisOptions)
			throws Exception {
		return configure(GeoItemWriter.<Map<String, Object>>builder().client(client)
				.poolConfig(redisOptions.poolConfig()).longitudeConverter(doubleFieldExtractor(longitudeField))
				.latitudeConverter(doubleFieldExtractor(latitudeField))).build();
	}

}
