package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;

import com.redis.riot.HelpCommand;
import com.redis.riot.RedisCommand;
import com.redis.riot.convert.CompositeConverter;
import com.redis.riot.convert.FieldExtractor;
import com.redis.riot.convert.ObjectToNumberConverter;
import com.redis.riot.convert.ObjectToStringConverter;
import com.redis.spring.batch.support.convert.KeyMaker;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@CommandLine.Command(sortOptions = false, abbreviateSynopsis = true)
public abstract class AbstractRedisCommand<O> extends HelpCommand implements RedisCommand<O> {

	@CommandLine.Option(names = { "-s",
			"--separator" }, description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keySeparator = ":";
	@CommandLine.Option(names = { "-r",
			"--remove" }, description = "Remove key or member fields the first time they are used")
	private boolean removeFields;

	protected Converter<Map<String, Object>, Double> doubleFieldExtractor(String field) {
		return numberFieldExtractor(Double.class, field, null);
	}

	protected Converter<Map<String, Object>, Object> fieldExtractor(String field, Object defaultValue) {
		return FieldExtractor.builder().field(field).remove(removeFields).defaultValue(defaultValue).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Converter<Map<String, Object>, String> stringFieldExtractor(String field) {
		Converter<Map<String, Object>, String> extractor = (Converter) fieldExtractor(field, null);
		if (extractor == null) {
			return null;
		}
		return new CompositeConverter(extractor, new ObjectToStringConverter());
	}

	@SuppressWarnings("unchecked")
	protected <T extends Number> Converter<Map<String, Object>, T> numberFieldExtractor(Class<T> targetType,
			String field, T defaultValue) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, defaultValue);
		return new CompositeConverter(extractor, new ObjectToNumberConverter<>(targetType));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected KeyMaker<Map<String, Object>> idMaker(String prefix, String[] fields) {
		KeyMaker.KeyMakerBuilder<Map<String, Object>> builder = KeyMaker.<Map<String, Object>>builder()
				.separator(keySeparator).prefix(prefix);
		if (!ObjectUtils.isEmpty(fields)) {
			Converter[] converters = new Converter[fields.length];
			for (int index = 0; index < fields.length; index++) {
				Converter<Map<String, Object>, Object> extractor = FieldExtractor.builder().remove(removeFields)
						.field(fields[index]).build();
				CompositeConverter converter = new CompositeConverter(extractor, new ObjectToStringConverter());
				converters[index] = converter;
			}
			builder.converters(converters);
		}
		return builder.build();
	}

}
