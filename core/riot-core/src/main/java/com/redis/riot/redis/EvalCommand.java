package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.riot.convert.MapToStringArrayConverter;
import com.redis.spring.batch.writer.operation.Eval;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "eval", description = "Evaluate a Lua script with keys and arguments from input")
public class EvalCommand extends AbstractRedisCommand<Map<String, Object>> {

	private static final String[] EMPTY_STRING = new String[0];

	@Option(names = "--sha", description = "Digest", paramLabel = "<sha>")
	private String sha;
	@Option(arity = "1..*", names = "--keys", description = "Key fields", paramLabel = "<names>")
	private String[] keys;
	@Option(arity = "1..*", names = "--args", description = "Arg fields", paramLabel = "<names>")
	private String[] args;
	@Option(names = "--output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;

	@Override
	public Eval<String, String, Map<String, Object>> operation() {
		return new Eval<>(sha, outputType, converter(keys), converter(args));
	}

	@SuppressWarnings("unchecked")
	private Converter<Map<String, Object>, String[]> converter(String[] fields) {
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
