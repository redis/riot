package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.StringWriter;
import com.redislabs.riot.redis.writer.StringWriter.StringFormat;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "string", description = "String data structure")
public class StringImportSubSubCommand extends AbstractRedisDataStructureImportSubSubCommand {

	@Option(names = "--format", description = "Serialization format: ${COMPLETION-CANDIDATES}", order = 5)
	private StringFormat format = StringFormat.Json;
	@Option(names = "--xml-root", description = "XML root element name.", order = 6)
	private String xmlRoot;

	@Override
	protected StringWriter doCreateWriter() {
		StringWriter writer = new StringWriter();
		writer.setFormat(format);
		writer.setRoot(xmlRoot);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "strings";
	}

}
