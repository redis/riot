package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.GeoItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd")
public class GeoaddCommand extends AbstractCollectionCommand {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return configure(GeoItemWriter.<Map<String, Object>>builder().client(client).poolConfig(poolConfig)
				.longitudeConverter(doubleFieldExtractor(longitudeField))
				.latitudeConverter(doubleFieldExtractor(latitudeField))).build();
	}

}
