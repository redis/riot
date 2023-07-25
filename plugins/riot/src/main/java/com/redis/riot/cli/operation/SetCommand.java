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
import picocli.CommandLine.Option;

@Command(name = "set", description = "Set strings from input")
public class SetCommand extends AbstractKeyCommand {

	public static final StringFormat DEFAULT_FORMAT = StringFormat.JSON;

	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<fmt>")
	private StringFormat format = DEFAULT_FORMAT;

	@Option(names = "--field", description = "Raw value field.", paramLabel = "<field>")
	private Optional<String> field = Optional.empty();

	@Option(names = "--root", description = "XML root element name.", paramLabel = "<name>")
	private String root;

	@Override
	public Set<String, String, Map<String, Object>> operation() {
		return new Set<>(key(), stringValueConverter());
	}

	private Function<Map<String, Object>, String> stringValueConverter() {
		switch (format) {
		case RAW:
			if (!field.isPresent()) {
				throw new IllegalArgumentException("Raw value field name not set");
			}
			return stringFieldExtractor(field.get());
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
