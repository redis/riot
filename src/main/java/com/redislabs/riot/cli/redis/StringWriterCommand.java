package com.redislabs.riot.cli.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.AbstractStringWriter;
import com.redislabs.riot.redis.writer.StringFieldWriter;
import com.redislabs.riot.redis.writer.StringObjectWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "string", description = "Redis string data structure")
public class StringWriterCommand extends AbstractDataStructureWriterCommand {

	public static enum StringFormat {
		raw, xml, json
	}

	@Option(names = "--format", description = "Serialization format: ${COMPLETION-CANDIDATES}")
	private StringFormat format = StringFormat.json;
	@Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String root;
	@Option(names = "--value", description = "Field to use for value when using raw format", paramLabel = "<field>")
	private String field;

	protected AbstractStringWriter writer() {
		switch (format) {
		case raw:
			return new StringFieldWriter(field);
		case xml:
			return new StringObjectWriter(objectWriter(new XmlMapper()));
		default:
			return new StringObjectWriter(objectWriter(new ObjectMapper()));
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(root);
	}

}