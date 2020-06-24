package com.redislabs.riot;

import lombok.Getter;
import picocli.CommandLine;

import java.util.List;

public class RediSearchExportOptions extends RediSearchOptions {

    @Getter
    @CommandLine.Option(names = "--query", description = "RediSearch query", paramLabel = "<string>")
    private String query;
    @Getter
    @CommandLine.Option(names = "--ft-options", arity = "1..*", description = "Search/aggregate options", paramLabel = "<opts>")
    private List<String> options;

}
