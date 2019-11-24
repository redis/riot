package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.writer.Geoadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description="Add geospatial items to specified keys")
public class GeoaddCommand extends AbstractCollectionRedisCommand {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@SuppressWarnings("rawtypes")
	@Override
	protected Geoadd collectionWriter() {
		return new Geoadd().longitudeField(longitudeField).latitudeField(latitudeField);
	}

}
