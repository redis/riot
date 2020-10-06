package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "geoadd")
public class GeoaddCommand extends AbstractCollectionCommand {

	@CommandLine.Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@CommandLine.Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;

	@Override
	protected AbstractCollectionWriter<String, String, Map<String, Object>> collectionWriter() {
		Geoadd<String, String, Map<String, Object>> writer = new Geoadd<>();
		writer.setLongitudeConverter(doubleFieldExtractor(longitudeField));
		writer.setLatitudeConverter(doubleFieldExtractor(latitudeField));
		return writer;
	}

}
