package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "zadd", description = "Add members with scores sorted sets")
public class ZaddCommand extends AbstractCollectionCommand {

    @Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String scoreField;
    @Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double scoreDefault = 1;

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configure(CommandBuilder.zadd()).scoreConverter(numberFieldExtractor(Double.class, scoreField, scoreDefault)).build();
    }

}
