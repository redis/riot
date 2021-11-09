package com.redis.riot.redis;

import org.springframework.batch.core.job.flow.Flow;

import picocli.CommandLine.Command;

@Command(name = "compare", description = "Compare a target Redis database with a source Redis database and prints the differences")
public class CompareCommand extends AbstractTargetCommand {

	@Override
	protected Flow flow() throws Exception {
		return verificationFlow();
	}

}
