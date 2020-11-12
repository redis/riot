package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisGeoItemWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd")
public class GeoaddCommand extends AbstractCollectionCommand {

    @Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;

    @Override
    public RedisGeoItemWriter<Map<String, Object>> writer() throws Exception {
	return configure(RedisGeoItemWriter.<Map<String, Object>>builder()
		.longitudeConverter(doubleFieldExtractor(longitudeField))
		.latitudeConverter(doubleFieldExtractor(latitudeField))).build();
    }

}
