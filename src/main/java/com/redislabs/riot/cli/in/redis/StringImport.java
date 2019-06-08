package com.redislabs.riot.cli.in.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.StringWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "string", description = "String data structure")
public class StringImport extends AbstractSingleImport {

	public static enum StringFormat {
		Xml, Json
	}

	@Option(names = "--format", description = "Serialization format: ${COMPLETION-CANDIDATES}")
	private StringFormat format = StringFormat.Json;
	@Option(names = "--root", description = "XML root element name.")
	private String root;

	@Override
	protected StringWriter redisItemWriter() {
		return new StringWriter(objectWriter().withRootName(root));
	}

	private ObjectWriter objectWriter() {
		if (format == StringFormat.Xml) {
			return new XmlMapper().writer();
		}
		return new ObjectMapper().writer();
	}

	@Override
	protected String getDataStructure() {
		return "strings";
	}

}
