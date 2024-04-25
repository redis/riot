package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.SetBuilder;
import com.redis.riot.core.operation.SetBuilder.StringFormat;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "set", description = "Set strings from input")
public class SetCommand extends AbstractRedisOperationCommand {

	public static final StringFormat DEFAULT_FORMAT = StringFormat.JSON;

	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<fmt>")
	private StringFormat format = DEFAULT_FORMAT;

	@Option(names = "--field", description = "Raw value field.", paramLabel = "<field>")
	private String field;

	@Option(names = "--root", description = "XML root element name.", paramLabel = "<name>")
	private String root;

	@Override
	protected SetBuilder operationBuilder() {
		SetBuilder supplier = new SetBuilder();
		supplier.setField(field);
		supplier.setFormat(format);
		supplier.setRoot(root);
		return supplier;
	}

}