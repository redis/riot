package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.GeoWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geo", description = "Redis geo data structure")
public class GeoWriterCommand extends AbstractCollectionWriterCommand {

	@Option(names = "--lon", required = true, description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", required = true, description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	protected GeoWriter collectionWriter() {
		return new GeoWriter(longitudeField, latitudeField);
	}

}
