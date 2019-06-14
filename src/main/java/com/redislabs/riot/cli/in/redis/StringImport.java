package com.redislabs.riot.cli.in.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.AbstractStringWriter;
import com.redislabs.riot.redis.writer.StringFieldWriter;
import com.redislabs.riot.redis.writer.StringObjectWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "string", description = "String data structure")
public class StringImport extends AbstractSingleImport {

	public static enum StringFormat {
		Raw, Xml, Json
	}

	@Option(names = "--format", description = "Serialization format: ${COMPLETION-CANDIDATES}. (default: ${DEFAULT-VALUE}).")
	private StringFormat format = StringFormat.Json;
	@Option(names = "--root", description = "XML root element name.")
	private String root;
	@Option(names = "--field", description = "Field to use for value when using raw format")
	private String field;

	@Override
	protected AbstractStringWriter redisItemWriter() {
		switch (format) {
		case Raw:
			return new StringFieldWriter(field);
		case Xml:
			return new StringObjectWriter(objectWriter(new XmlMapper()));
		default:
			return new StringObjectWriter(objectWriter(new ObjectMapper()));
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(root);
	}

	@Override
	protected String getDataStructure() {
		return "strings";
	}

}
