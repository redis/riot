package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class GeneratorOptions {

    @Getter
    @CommandLine.Option(names = "--faker", arity = "1..*", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
    private Map<String, String> fakerFields = new LinkedHashMap<>();
    @Getter
    @CommandLine.Option(names = "--faker-help", description = "Show all available Faker properties")
    private boolean fakerHelp;
    @Getter
    @CommandLine.Option(names = "--faker-index", description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;
    @Getter
    @CommandLine.Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @Getter
    @CommandLine.Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;

    public boolean isSet() {
        return !fakerFields.isEmpty() || fakerIndex != null;
    }

}
