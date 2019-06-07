package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.GeoWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geo", description = "Geo data structure")
public class GeoImport extends AbstractCollectionImport {

	@Option(names = "--longitude", description = "Longitude field for geo sets.")
	private String longitude;
	@Option(names = "--latitude", description = "Latitude field for geo sets.")
	private String latitude;

	@Override
	protected AbstractCollectionRedisItemWriter collectionRedisItemWriter() {
		GeoWriter writer = new GeoWriter();
		writer.setLatitudeField(latitude);
		writer.setLongitudeField(longitude);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "geoset";
	}

}
