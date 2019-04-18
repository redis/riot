package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.GeoWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geo", description = "Geo data structure")
public class GeoImportSubSubCommand extends AbstractCollectionRedisImportSubSubCommand {

	@Option(names = "--longitude-field", description = "Longitude field for geo sets.")
	private String longitudeField;
	@Option(names = "--latitude-field", description = "Latitude field for geo sets.")
	private String latitudeField;

	@Override
	protected AbstractCollectionRedisItemWriter collectionRedisItemWriter() {
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
