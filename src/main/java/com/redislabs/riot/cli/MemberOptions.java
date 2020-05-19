package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;

public class MemberOptions {
    @Getter
    @CommandLine.Option(names = "--member-space", description = "Prefix for member IDs", paramLabel = "<str>")
    private String keyspace;
    @Getter
    @CommandLine.Option(names = "--members", arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
    private String[] fields = new String[0];

}
