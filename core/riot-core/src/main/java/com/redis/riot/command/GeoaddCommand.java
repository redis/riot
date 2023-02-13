package com.redis.riot.command;

import java.util.Map;

import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.writer.operation.Geoadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractCollectionCommand {

	@Mixin
	private GeoaddOptions options = new GeoaddOptions();

	@Override
	public Geoadd<String, String, Map<String, Object>> operation() {
		return Geoadd.<String, Map<String, Object>>key(key()).value(new GeoValueConverter<>(member(),
				doubleFieldExtractor(options.getLongitudeField()), doubleFieldExtractor(options.getLatitudeField())))
				.build();
	}

}
