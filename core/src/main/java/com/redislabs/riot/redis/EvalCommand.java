package com.redislabs.riot.redis;

import com.redislabs.riot.convert.MapToStringArrayConverter;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "eval", description = "Evaluate a Lua script with keys and arguments from input")
public class EvalCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Option(names = "--sha", description = "Digest", paramLabel = "<sha>")
    private String sha;
    @Option(names = "--keys", arity = "1..*", description = "Key fields", paramLabel = "<names>")
    private String[] keys = new String[0];
    @Option(names = "--args", arity = "1..*", description = "Arg fields", paramLabel = "<names>")
    private String[] args = new String[0];
    @Option(names = "--output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
    private ScriptOutputType outputType = ScriptOutputType.STATUS;

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return CommandBuilder.<Map<String, Object>>eval().sha(sha).outputType(outputType).keysConverter(mapToArrayConverter(keys)).argsConverter(mapToArrayConverter(args)).build();
    }

    @SuppressWarnings("unchecked")
    private MapToStringArrayConverter mapToArrayConverter(String[] fields) {
        Converter<Map<String, Object>, String>[] extractors = new Converter[fields.length];
        for (int index = 0; index < fields.length; index++) {
            extractors[index] = stringFieldExtractor(fields[index]);
        }
        return new MapToStringArrayConverter(extractors);
    }

}
