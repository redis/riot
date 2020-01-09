package com.redislabs.riot.cli.redis.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.map.AbstractKeyMapRedisWriter;
import com.redislabs.riot.redis.writer.map.SetField;
import com.redislabs.riot.redis.writer.map.SetObject;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "set", description = "Set string values")
public class SetCommand extends AbstractKeyRedisCommand {

	public enum StringFormat {
		raw, xml, json
	}

	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat format = StringFormat.json;
	@Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String root;
	@Option(names = "--value", description = "Value field for raw format", paramLabel = "<field>")
	private String value;

	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractKeyMapRedisWriter keyWriter() {
		switch (format) {
		case raw:
			SetField setField = new SetField();
			setField.setField(value);
			return setField;
		case xml:
			SetObject setXml = new SetObject();
			setXml.setObjectWriter(objectWriter(new XmlMapper()));
			return setXml;
		default:
			SetObject setObject = new SetObject();
			setObject.setObjectWriter(objectWriter(new ObjectMapper()));
			return setObject;
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(root);
	}

}
