package com.redislabs.riot.cli.in.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.ListWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list", description = "List data structure")
public class ListImport extends AbstractCollectionImport {

	public enum PushDirection {
		Left, Right
	}

	@Option(names = "--direction", description = "Direction for list push: ${COMPLETION-CANDIDATES}. (default: ${DEFAULT-VALUE}).")
	private PushDirection direction = PushDirection.Left;

	@Override
	protected AbstractCollectionRedisItemWriter collectionRedisItemWriter() {
		return new ListWriter();
	}

	@Override
	protected String getDataStructure() {
		return "list";
	}

}
