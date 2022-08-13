package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.riot.convert.MapToStringArrayConverter;
import com.redis.spring.batch.writer.operation.Eval;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "eval", description = "Evaluate a Lua script with keys and arguments from input")
public class EvalCommand extends AbstractOperationCommand<Map<String, Object>> {

	private static final String[] EMPTY_STRING = new String[0];

	@Mixin
	private EvalOptions options = new EvalOptions();

	@Override
	public Eval<String, String, Map<String, Object>> operation() {
		return new Eval<>(options.getSha(), options.getOutputType(), converter(options.getKeys()),
				converter(options.getArgs()));
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
