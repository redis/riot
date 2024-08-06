package com.redis.riot.operation;

import java.util.Map;

import com.redis.spring.batch.item.redis.writer.impl.Hset;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "hset", description = "Set hashes from input")
public class HsetCommand extends AbstractOperationCommand {

	@ArgGroup(exclusive = false)
	private FieldFilterArgs fieldFilterArgs = new FieldFilterArgs();

	@Override
	public Hset<String, String, Map<String, Object>> operation() {
		return new Hset<>(keyFunction(), fieldFilterArgs.mapFunction());
	}

	public FieldFilterArgs getFieldFilterArgs() {
		return fieldFilterArgs;
	}

	public void setFieldFilterArgs(FieldFilterArgs filteringArgs) {
		this.fieldFilterArgs = filteringArgs;
	}

}