package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.convert.ObjectMapperConverter;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "set")
public class SetCommand extends AbstractKeyCommand {

	private enum StringFormat {
		RAW, XML, JSON
	}

	@CommandLine.Option(names = "--format", defaultValue = "JSON", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat format;
	@CommandLine.Option(names = "--field", description = "String value field", paramLabel = "<field>")
	private String field;
	@CommandLine.Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String root;

	@Override
	protected AbstractKeyWriter<String, String, Map<String, Object>> keyWriter() {
		Set<String, String, Map<String, Object>> writer = new Set<>();
		writer.setValueConverter(stringValueConverter());
		return writer;
	}

	private Converter<Map<String, Object>, String> stringValueConverter() {
		switch (format) {
		case RAW:
			return stringFieldExtractor(field);
		case XML:
			return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(root));
		case JSON:
			ObjectMapper jsonMapper = new ObjectMapper();
			jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			jsonMapper.setSerializationInclusion(Include.NON_NULL);
			return new ObjectMapperConverter<>(jsonMapper.writer().withRootName(root));
		}
		throw new IllegalArgumentException("Unsupported String format: " + format);
	}

}
