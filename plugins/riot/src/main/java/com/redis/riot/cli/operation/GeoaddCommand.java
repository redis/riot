package com.redis.riot.cli.operation;

import com.redis.riot.core.operation.GeoaddBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractCollectionOperationCommand {

    @Option(names = "--lon", required = true, description = "Longitude field.", paramLabel = "<field>")
    private String longitude;

    @Option(names = "--lat", required = true, description = "Latitude field.", paramLabel = "<field>")
    private String latitude;

    @Override
    protected GeoaddBuilder collectionOperationBuilder() {
        GeoaddBuilder builder = new GeoaddBuilder();
        builder.latitude(latitude);
        builder.longitude(longitude);
        return builder;
    }

}
