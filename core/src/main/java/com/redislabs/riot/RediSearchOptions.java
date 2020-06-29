package com.redislabs.riot;

import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RediSearchOptions {

    @CommandLine.Option(names = "--index", description = "Name of the search/suggest index", paramLabel = "<name>")
    private String index;

}
