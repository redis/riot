package com.redis.riot.cli;

import org.springframework.expression.Expression;

import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.TemplateExpression;

import picocli.CommandLine.Option;

public class KeyValueProcessorArgs {

    @Option(names = "--key-proc", description = "SpEL template expression to transform the name of each key. E.g. --key-proc=\"#{#source.database}:#{key}\" transform key 'test:1' into '0:test:1'", paramLabel = "<exp>")
    TemplateExpression keyExpression;

    @Option(names = "--type-proc", description = "SpEL expression to transform the type of each key.", paramLabel = "<exp>")
    Expression typeExpression;

    @Option(names = "--ttl-proc", description = "SpEL expression to transform the TTL of each key.", paramLabel = "<exp>")
    Expression ttlExpression;

    @Option(names = "--no-ttl", description = "Ignore key expiration TTLs from source instead of passing them along to the target.")
    boolean dropTtl;

    @Option(names = "--no-stream-id", description = "Drop IDs from source stream messages instead of passing them along to the target.")
    boolean dropStreamMessageIds;

    public KeyValueProcessorOptions processorOptions() {
        KeyValueProcessorOptions options = new KeyValueProcessorOptions();
        options.setKeyExpression(keyExpression);
        options.setTtlExpression(ttlExpression);
        options.setTypeExpression(typeExpression);
        options.setDropTtl(dropTtl);
        options.setDropStreamMessageId(dropStreamMessageIds);
        return options;
    }

}
