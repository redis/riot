package com.redis.riot.redis;

import org.springframework.batch.core.Job;

import picocli.CommandLine.Command;

@Command(name = "compare", description = "Compare 2 Redis databases and print the differences")
public class CompareCommand extends AbstractTargetCommand {

	private static final String NAME = "compare";

	@Override
	protected Job createJob(TargetCommandContext context) {
		return context.getJobRunner().job(NAME).start(verificationStep(context)).build();
	}

}
