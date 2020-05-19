package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;

public class ScoreOptions {

    @Getter
    @CommandLine.Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String field;
    @Getter
    @CommandLine.Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double defaultValue = 1d;

}
