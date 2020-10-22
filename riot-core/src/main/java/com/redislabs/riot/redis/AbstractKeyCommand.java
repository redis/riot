package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConverterFactory;

import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.FieldExtractor;
import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.ObjectToNumberConverter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractKeyCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Option(names = { "-s",
			"--separator" }, description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keySeparator = ":";
	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
	private String[] keys = new String[0];
	@Option(names = { "-r",
			"--remove" }, description = "Remove fields the first time they are used (key or member fields)")
	private boolean removeFields;

	@Override
	public AbstractKeyWriter<String, String, Map<String, Object>> writer() {
		AbstractKeyWriter<String, String, Map<String, Object>> writer = keyWriter();
		writer.setKeyConverter(idMaker(keyspace, keys));
		return writer;
	}

	protected abstract AbstractKeyWriter<String, String, Map<String, Object>> keyWriter();

	protected KeyMaker idMaker(String prefix, String[] fields) {
		Converter[] extractors = new Converter[fields.length];
		for (int index = 0; index < fields.length; index++) {
			extractors[index] = FieldExtractor.builder().remove(removeFields).field(fields[index]).build();
		}
		return KeyMaker.builder().separator(keySeparator).prefix(prefix).extractors(extractors).build();
	}

	protected Converter<Map<String, Object>, Double> doubleFieldExtractor(String field) {
		return numberFieldExtractor(Double.class, field, null);
	}

	protected Converter<Map<String, Object>, Object> fieldExtractor(String field, Object defaultValue) {
		return FieldExtractor.builder().field(field).remove(removeFields).defaultValue(defaultValue).build();
	}

	protected Converter<Map<String, Object>, String> stringFieldExtractor(String field) {
		Converter<Map<String, Object>, String> extractor = (Converter) fieldExtractor(field, null);
		if (extractor == null) {
			return null;
		}
		return new CompositeConverter(extractor, ConverterFactory.getObjectToStringConverter());
	}

	protected <T extends Number> Converter<Map<String, Object>, T> numberFieldExtractor(Class<T> targetType,
			String field, T defaultValue) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, defaultValue);
		return new CompositeConverter(extractor, new ObjectToNumberConverter<>(targetType));
	}

}
