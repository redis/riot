package com.redislabs.riot.gen;

import lombok.Data;
import picocli.CommandLine;

import java.util.Locale;
import java.util.Map;

@Data
public class GenerateOptions {

    @CommandLine.Parameters(arity = "0..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
    private Map<String, String> fakerFields;
    @SuppressWarnings("unused")
    @CommandLine.Option(names = "--infer", description = "Introspect given RediSearch index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;
    @CommandLine.Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @SuppressWarnings("unused")
    @CommandLine.Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;
    @CommandLine.Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long start = 0;
    @CommandLine.Option(names = "--end", description = "End index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long end = 1000;
    @CommandLine.Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long sleep = 0;
}
