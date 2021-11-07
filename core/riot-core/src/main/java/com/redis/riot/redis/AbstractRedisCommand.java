package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.redis.riot.HelpCommand;
import com.redis.riot.RedisCommand;
import com.redis.riot.convert.CompositeConverter;
import com.redis.riot.convert.FieldExtractorFactory;
import com.redis.riot.convert.IdConverterBuilder;
import com.redis.riot.convert.ObjectToNumberConverter;

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
	@CommandLine.Option(names = "--ignore-missing", description = "Ignore missing fields")
	private boolean ignoreMissingFields;

	protected Converter<Map<String, Object>, Double> doubleFieldExtractor(String field) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractorFactory().field(field);
		return new CompositeConverter<>(extractor, new ObjectToNumberConverter<>(Double.class));
	}

	protected Converter<Map<String, Object>, String> stringFieldExtractor(String field) {
		return fieldExtractorFactory().string(field);
	}

	private FieldExtractorFactory fieldExtractorFactory() {
		return new FieldExtractorFactory().remove(removeFields).nullCheck(!ignoreMissingFields);
	}

	protected <T extends Number> Converter<Map<String, Object>, T> numberExtractor(String field, Class<T> targetType,
			T defaultValue) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractorFactory().field(field, defaultValue);
		return new CompositeConverter<>(extractor, new ObjectToNumberConverter<>(targetType));
	}

	protected Converter<Map<String, Object>, String> idMaker(String prefix, String... fields) {
		return new IdConverterBuilder().remove(removeFields).prefix(prefix).fields(fields).build();
	}

}
