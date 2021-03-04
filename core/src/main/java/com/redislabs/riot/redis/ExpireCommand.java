package com.redislabs.riot.redis;

import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

    @SuppressWarnings("unused")
    @Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeoutField;
    @Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long timeoutDefault = 60;

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configureKeyCommandBuilder(CommandBuilder.expire()).timeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault)).build();
    }

}
