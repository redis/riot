package com.redislabs.riot.cli.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.batch.redis.map.SetFieldMapWriter;
import com.redislabs.riot.batch.redis.map.SetMapWriter;
import com.redislabs.riot.batch.redis.map.SetObjectMapWriter;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class StringCommandOptions {

	@Option(names = "--string-format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat format = StringFormat.json;
	@Option(names = "--string-root", description = "XML root element name", paramLabel = "<name>")
	private String root;
	@Option(names = "--string-value", description = "Value field for raw format", paramLabel = "<field>")
	private String value;

	public <R> SetMapWriter<R> writer() {
		switch (format) {
		case raw:
			return new SetFieldMapWriter<R>().field(value);
		case xml:
			return new SetObjectMapWriter<R>().objectWriter(objectWriter(new XmlMapper()));
		default:
			return new SetObjectMapWriter<R>().objectWriter(objectWriter(new ObjectMapper()));
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(root);
	}

}
