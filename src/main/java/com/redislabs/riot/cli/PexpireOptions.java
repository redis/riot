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
public class PexpireOptions {

    @Builder.Default
    @Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long defaultTimeout = 60;
    @Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeout;

}
