package com.redislabs.riot.cli;

import com.redislabs.riot.convert.KeyMaker;
import lombok.Getter;
import picocli.CommandLine;

public class KeyOptions {
    @Getter
    @CommandLine.Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String separator = KeyMaker.DEFAULT_SEPARATOR;
    @Getter
    @CommandLine.Option(names = {"-p", "--keyspace"}, description = "Keyspace prefix", paramLabel = "<str>")
    private String keyspace;
    @Getter
    @CommandLine.Option(names = {"-k", "--keys"}, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
    private String[] keyFields = new String[0];
    @Getter
    @CommandLine.Option(names = "--keys-keep", description = "Keep key fields in data structure")
    private boolean keepKeyFields;
}
