package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.convert.ObjectMapperConverter;
import com.redis.spring.batch.writer.operation.Set;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "set", description = "Set strings from input")
public class SetCommand extends AbstractKeyCommand {

	@Mixin
	private SetOptions options = new SetOptions();

	@Override
	public Set<String, String, Map<String, Object>> operation() {
		return new Set<>(key(), stringValueConverter());
	}

	private Function<Map<String, Object>, String> stringValueConverter() {
		switch (options.getFormat()) {
		case RAW:
			Optional<String> field = options.getField();
			if (!field.isPresent()) {
				throw new IllegalArgumentException("Raw value field name not set");
			}
			return stringFieldExtractor(field.get());
		case XML:
			return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(options.getRoot()));
		default:
			ObjectMapper jsonMapper = new ObjectMapper();
			jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			jsonMapper.setSerializationInclusion(Include.NON_NULL);
			return new ObjectMapperConverter<>(jsonMapper.writer().withRootName(options.getRoot()));
		}
	}

}
