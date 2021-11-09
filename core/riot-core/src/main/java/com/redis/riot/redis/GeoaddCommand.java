package com.redis.riot.redis;

import java.util.Map;

import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.convert.GeoValueConverter;
import com.redis.spring.batch.support.operation.Geoadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractCollectionCommand {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Geoadd.<String, String, Map<String, Object>>key(key()).value(new GeoValueConverter<>(member(),
				doubleFieldExtractor(longitudeField), doubleFieldExtractor(latitudeField))).build();
	}

}
