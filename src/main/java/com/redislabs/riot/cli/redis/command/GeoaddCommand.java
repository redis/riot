package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.map.Geoadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description = "Add geospatial items to specified keys")
public class GeoaddCommand extends AbstractCollectionRedisCommand {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	protected Geoadd collectionWriter() {
		Geoadd writer = new Geoadd();
		writer.longitudeField(longitudeField);
		writer.latitudeField(latitudeField);
		return writer;
	}

}
