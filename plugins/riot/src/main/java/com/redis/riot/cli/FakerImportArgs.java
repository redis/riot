package com.redis.riot.cli;

import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;

import com.redis.riot.faker.FakerImport;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FakerImportArgs {

    @Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int count = FakerImport.DEFAULT_COUNT;

    @Parameters(arity = "0..*", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "SPEL")
    Map<String, Expression> fields;

    @Option(names = "--infer", description = "Introspect given RediSearch index to infer Faker fields.", paramLabel = "<name>")
    String searchIndex;

    @Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE}).", paramLabel = "<tag>")
    Locale locale = FakerImport.DEFAULT_LOCALE;

}
