package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "geoadd", description = "Add geospatial items")
public class GeoaddCommand extends AbstractCollectionCommand {

    @Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configure(CommandBuilder.geoadd()).longitudeConverter(doubleFieldExtractor(longitudeField)).latitudeConverter(doubleFieldExtractor(latitudeField)).build();
    }

}
