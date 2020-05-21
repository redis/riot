package com.redislabs.riot.cli;

import lombok.*;
import picocli.CommandLine.Option;

public class GeoaddOptions {

    @Getter
    @Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @Getter
    @Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;

}
