package com.redislabs.riot;

import lombok.Getter;
import picocli.CommandLine;

public class RediSearchOptions {

    @Getter
    @CommandLine.Option(names = {"-i", "--index"}, description = "Name of the RediSearch index", paramLabel = "<name>")
    private String index;
}
