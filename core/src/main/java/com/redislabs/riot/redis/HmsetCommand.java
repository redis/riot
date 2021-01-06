package com.redislabs.riot.redis;

import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;
import io.lettuce.core.RedisFuture;
import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine.Command;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "hmset", aliases = "h", description = "Set hashes from input")
public class HmsetCommand extends AbstractKeyCommand {

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return configure(CommandBuilder.hmset()).mapConverter(new MapFlattener<>(new ObjectToStringConverter())).build();
    }

}
