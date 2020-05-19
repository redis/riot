package com.redislabs.riot.cli;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine.Option;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GeoaddOptions {

    @Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitude;
    @Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitude;

}
