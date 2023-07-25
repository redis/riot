package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

import com.redis.spring.batch.convert.GeoValueConverter;
import com.redis.spring.batch.writer.operation.Geoadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractCollectionCommand {

	@Option(names = "--lon", required = true, description = "Longitude field.", paramLabel = "<field>")
	private String longitude;

	@Option(names = "--lat", required = true, description = "Latitude field.", paramLabel = "<field>")
	private String latitude;

	@Override
	public Geoadd<String, String, Map<String, Object>> operation() {
		ToDoubleFunction<Map<String, Object>> lon = doubleExtractor(Optional.of(longitude), 0);
		ToDoubleFunction<Map<String, Object>> lat = doubleExtractor(Optional.of(latitude), 0);
		return new Geoadd<>(key(), new GeoValueConverter<>(member(), lon, lat));
	}

}
