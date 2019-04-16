package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.GeoWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geo", description = "Geo data structure")
public class GeoImportSubSubCommand extends AbstractRedisCollectionImportSubSubCommand {

	@Option(names = "--longitude-field", description = "Longitude field for geo sets.", order = 4)
	private String longitudeField;
	@Option(names = "--latitude-field", description = "Latitude field for geo sets.", order = 4)
	private String latitudeField;

	@Override
	protected GeoWriter doCreateWriter() {
		GeoWriter writer = new GeoWriter();
		writer.setLatitudeField(latitudeField);
		writer.setLongitudeField(longitudeField);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "geoset";
	}

}
