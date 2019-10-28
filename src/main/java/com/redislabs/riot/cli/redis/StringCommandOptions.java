package com.redislabs.riot.cli.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.batch.redis.writer.SetFieldMapWriter;
import com.redislabs.riot.batch.redis.writer.SetMapWriter;
import com.redislabs.riot.batch.redis.writer.SetObjectMapWriter;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class StringCommandOptions {

	@Option(names = "--string-format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat format = StringFormat.json;
	@Option(names = "--string-root", description = "XML root element name", paramLabel = "<name>")
	private String root;
	@Option(names = "--string-value", description = "Value field for raw format", paramLabel = "<field>")
	private String value;

	public SetMapWriter writer() {
		switch (format) {
		case raw:
			SetFieldMapWriter fieldWriter = new SetFieldMapWriter();
			fieldWriter.setField(value);
			return fieldWriter;
		case xml:
			SetObjectMapWriter xmlWriter = new SetObjectMapWriter();
			xmlWriter.setObjectWriter(objectWriter(new XmlMapper()));
			return xmlWriter;
		default:
			SetObjectMapWriter jsonWriter = new SetObjectMapWriter();
			jsonWriter.setObjectWriter(objectWriter(new ObjectMapper()));
			return jsonWriter;
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(root);
	}

}
