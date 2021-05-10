package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractCollectionCommand {

    @SuppressWarnings("unused")
    @Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @SuppressWarnings("unused")
    @Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureCollectionCommandBuilder(RedisOperation.geoadd()).longitude(doubleFieldExtractor(longitudeField)).latitude(doubleFieldExtractor(latitudeField)).build();
    }

}
