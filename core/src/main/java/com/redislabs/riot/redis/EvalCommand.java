package com.redislabs.riot.redis;

import com.redislabs.riot.convert.MapToStringArrayConverter;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.function.BiFunction;

@Command(name = "eval", description = "Evaluate a Lua script with keys and arguments from input")
public class EvalCommand extends AbstractRedisCommand<Map<String, Object>> {

    private final static String[] EMPTY_STRING = new String[0];

    @SuppressWarnings("unused")
    @Option(names = "--sha", description = "Digest", paramLabel = "<sha>")
    private String sha;
    @Option(arity = "1..*", names = "--keys", description = "Key fields", paramLabel = "<names>")
    private String[] keys;
    @Option(arity = "1..*", names = "--args", description = "Arg fields", paramLabel = "<names>")
    private String[] args;
    @Option(names = "--output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
    private ScriptOutputType outputType = ScriptOutputType.STATUS;

    @Override
    public BiFunction<?, Map<String, Object>, RedisFuture<?>> command() {
        return CommandBuilder.<Map<String, Object>>eval().sha(sha).outputType(outputType).keysConverter(mapToArrayConverter(keys)).argsConverter(mapToArrayConverter(args)).build();
    }

    @SuppressWarnings("unchecked")
    private Converter<Map<String, Object>, String[]> mapToArrayConverter(String[] fields) {
        if (ObjectUtils.isEmpty(fields)) {
            return m -> EMPTY_STRING;
        }
        Converter<Map<String, Object>, String>[] extractors = new Converter[fields.length];
        for (int index = 0; index < fields.length; index++) {
            extractors[index] = stringFieldExtractor(fields[index]);
        }
        return new MapToStringArrayConverter(extractors);
    }

}
