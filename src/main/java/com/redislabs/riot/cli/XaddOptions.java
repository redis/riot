package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;

public class XaddOptions {

    @Getter
    @CommandLine.Option(names = "--trim", description = "Stream efficient trimming (~ flag)")
    private boolean trim;
    @Getter
    @CommandLine.Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @Getter
    @CommandLine.Option(names = "--stream-id", description = "Stream entry ID field", paramLabel = "<field>")
    private String id;
}
