package com.redis.riot.replicate;

import org.springframework.batch.core.Job;

import com.redis.riot.JobCommandContext;

import picocli.CommandLine.Command;

@Command(name = "compare", description = "Compare 2 Redis databases and print the differences")
public class CompareCommand extends AbstractTargetCommand {

	private static final String NAME = "compare";

	@Override
	protected Job job(JobCommandContext context) {
		return context.job(NAME).start(verificationStep((TargetCommandContext) context)).build();
	}

}
