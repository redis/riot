package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.List;

public class FtOptions {

    @Getter
    @Option(names = {"-i", "--index"}, description = "Name of the RediSearch index", paramLabel = "<name>")
    private String index;
    @Getter
    @Option(names = "--query", description = "RediSearch query", paramLabel = "<string>")
    private String query;
    @Getter
    @Option(names = "--ft-options", arity = "1..*", description = "Search/aggregate options", paramLabel = "<opts>")
    private List<String> options;
    @Getter
    @CommandLine.Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
    private String payloadField;

}
