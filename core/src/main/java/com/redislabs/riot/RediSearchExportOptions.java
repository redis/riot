package com.redislabs.riot;

import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RediSearchExportOptions extends RediSearchOptions {

    @CommandLine.Option(names = "--query", description = "RediSearch query", paramLabel = "<string>")
    private String query;
    @CommandLine.Option(names = "--ft-options", arity = "1..*", description = "Search/aggregate options", paramLabel = "<str>")
    private Object[] options;
}
