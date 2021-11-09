package com.redis.riot.redis;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.convert.ObjectMapperConverter;
import com.redis.spring.batch.support.RedisOperation;
import com.redis.spring.batch.support.operation.Set;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "set", description = "Set strings from input")
public class SetCommand extends AbstractKeyCommand {

	private enum StringFormat {
		RAW, XML, JSON
	}

	private static final Logger log = LoggerFactory.getLogger(SetCommand.class);

	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat format = StringFormat.JSON;
	@Option(names = "--field", description = "Raw value field", paramLabel = "<field>")
	private String field;
	@Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String root;

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return Set.<String, String, Map<String, Object>>key(key()).value(stringValueConverter()).build();
	}

	private Converter<Map<String, Object>, String> stringValueConverter() {
		switch (format) {
		case RAW:
			if (field == null) {
				log.warn("Raw value field name not set");
			}
			return stringFieldExtractor(field);
		case XML:
			return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(root));
		default:
			ObjectMapper jsonMapper = new ObjectMapper();
			jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			jsonMapper.setSerializationInclusion(Include.NON_NULL);
			return new ObjectMapperConverter<>(jsonMapper.writer().withRootName(root));
		}
	}

}
