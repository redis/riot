package com.redislabs.riot.cli;

import lombok.*;
import picocli.CommandLine.Option;

public class PexpireOptions {

    @Getter
    @Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long defaultTimeout = 60;
    @Getter
    @Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeout;

}
