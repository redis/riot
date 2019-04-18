package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.ListWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list", description = "List data structure")
public class ListImportSubSubCommand extends AbstractCollectionRedisImportSubSubCommand {

	public enum PushDirection {
		Left, Right
	}

	@Option(names = "--push-direction", description = "Direction for list push: ${COMPLETION-CANDIDATES}. (default: ${DEFAULT-VALUE}).")
	private PushDirection pushDirection = PushDirection.Left;

	@Override
	protected AbstractCollectionRedisItemWriter collectionRedisItemWriter() {
		return new ListWriter();
	}

	@Override
	protected String getDataStructure() {
		return "list";
	}

}
