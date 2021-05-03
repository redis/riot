package com.redislabs.riot.gen;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine;

import java.util.Locale;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GenerateOptions {

    @CommandLine.Parameters(arity = "0..*", description = "SpEL expressions", paramLabel = "SPEL")
    private Map<String, String> fakerFields;
    @SuppressWarnings("unused")
    @CommandLine.Option(names = "--infer", description = "Introspect given RediSearch index to introspect Faker fields", paramLabel = "<index>")
    private String fakerIndex;
    @Builder.Default
    @CommandLine.Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @SuppressWarnings("unused")
    @CommandLine.Option(names = "--metadata", description = "Include metadata (index, partition)")
    private boolean includeMetadata;
    @Builder.Default
    @CommandLine.Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long start = 0;
    @Builder.Default
    @CommandLine.Option(names = "--end", description = "End index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long end = 1000;
    @Builder.Default
    @CommandLine.Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long sleep = 0;
}
