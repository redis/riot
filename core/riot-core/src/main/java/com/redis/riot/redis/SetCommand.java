package com.redis.riot.redis;

import java.util.Map;
import java.util.Optional;

import org.springframework.core.convert.converter.Converter;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.convert.ObjectMapperConverter;
import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.Set;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "set", description = "Set strings from input")
public class SetCommand extends AbstractKeyCommand {

	@Mixin
	private SetOptions options = new SetOptions();

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Set.<String, String, Map<String, Object>>key(key()).value(stringValueConverter()).build();
	}

	private Converter<Map<String, Object>, String> stringValueConverter() {
		switch (options.getFormat()) {
		case RAW:
			Optional<String> field = options.getField();
			if (field.isEmpty()) {
				throw new RuntimeException("Raw value field name not set");
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
