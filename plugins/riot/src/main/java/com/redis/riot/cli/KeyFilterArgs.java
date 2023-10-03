package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.core.KeyFilterOptions;
import com.redis.spring.batch.common.Range;

import picocli.CommandLine.Option;

public class KeyFilterArgs {

    @Option(names = "--key-include", arity = "1..*", description = "Glob pattern to match keys for inclusion.", paramLabel = "<exp>")
    List<String> includes;

    @Option(names = "--key-exclude", arity = "1..*", description = "Glob pattern to match keys for exclusion.", paramLabel = "<exp>")
    List<String> excludes;

    @Option(names = "--key-slots", arity = "1..*", description = "Key slot ranges to filter keyspace notifications.", paramLabel = "<range>")
    List<Range> slots;

    public KeyFilterOptions keyFilterOptions() {
        KeyFilterOptions options = new KeyFilterOptions();
        options.setExcludes(excludes);
        options.setIncludes(includes);
        options.setSlots(slots);
        return options;
    }

}
