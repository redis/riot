package com.redis.riot.redis;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;

import picocli.CommandLine.Command;

@Command(name = "compare", description = "Compare 2 Redis databases and print the differences")
public class CompareCommand extends AbstractTargetCommand {

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		return jobBuilder.start(verificationStep()).build();
	}

}
