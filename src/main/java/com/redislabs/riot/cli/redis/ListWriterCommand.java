package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.ListLeftPushWriter;
import com.redislabs.riot.redis.writer.ListRightPushWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list", description="Redis list data structure")
public class ListWriterCommand extends AbstractCollectionWriterCommand {

	public enum PushDirection {
		left, right
	}

	@Option(names = "--push-direction", description = "Direction for list push: ${COMPLETION-CANDIDATES}")
	private PushDirection direction = PushDirection.left;

	@Override
	protected AbstractCollectionRedisItemWriter collectionWriter() {
		switch (direction) {
		case right:
			return new ListRightPushWriter();
		default:
			return new ListLeftPushWriter();
		}
	}

}
