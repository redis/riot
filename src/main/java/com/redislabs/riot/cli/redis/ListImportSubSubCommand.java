package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.ListWriter;
import com.redislabs.riot.redis.writer.ListWriter.PushDirection;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list", description = "List data structure")
public class ListImportSubSubCommand extends AbstractRedisCollectionImportSubSubCommand {

	@Option(names = "--push-direction", description = "Direction for list push: ${COMPLETION-CANDIDATES}. (default: ${DEFAULT-VALUE}).", order = 5)
	private PushDirection pushDirection = PushDirection.Left;

	@Override
	protected ListWriter doCreateWriter() {
		ListWriter writer = new ListWriter();
		writer.setPushDirection(pushDirection);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "list";
	}

}
