package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;

public class FtSugaddOptions {

    @Getter
    @CommandLine.Option(names = "--sug-field", description = "Field containing suggestion", paramLabel = "<field>")
    private String field;
    @Getter
    @CommandLine.Option(names = "--increment", description = "Use increment to set value")
    private boolean increment;

}
