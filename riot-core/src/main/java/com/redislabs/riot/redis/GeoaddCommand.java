package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd")
public class GeoaddCommand extends AbstractCollectionCommand {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	protected AbstractCollectionWriter<String, String, Map<String, Object>> collectionWriter() {
		Geoadd<String, String, Map<String, Object>> writer = new Geoadd<>();
		writer.setLongitudeConverter(doubleFieldExtractor(longitudeField));
		writer.setLatitudeConverter(doubleFieldExtractor(latitudeField));
		return writer;
	}

}
