package com.redis.riot.cli.operation;

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
		return new Geoadd<>(key(), new GeoValueConverter<>(member(), doubleFieldExtractor(options.getLongitude()),
				doubleFieldExtractor(options.getLatitude())));
	}

}
