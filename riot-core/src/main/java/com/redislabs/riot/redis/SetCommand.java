package com.redislabs.riot.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.StringItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.convert.ObjectMapperConverter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "set")
public class SetCommand extends AbstractKeyCommand {

	private enum StringFormat {
		RAW, XML, JSON
	}

	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat format = StringFormat.JSON;
	@Option(names = "--field", description = "String value field", paramLabel = "<field>")
	private String field;
	@Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String root;

	@Override
	public ItemWriter<Map<String, Object>> writer(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		return configure(StringItemWriter.<Map<String, Object>>builder().client(client).poolConfig(poolConfig)
				.valueConverter(stringValueConverter())).build();
	}

	private Converter<Map<String, Object>, String> stringValueConverter() {
		switch (format) {
		case RAW:
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
