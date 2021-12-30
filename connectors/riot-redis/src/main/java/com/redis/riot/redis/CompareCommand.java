package com.redis.riot.redis;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;

import picocli.CommandLine.Command;

@Command(name = "compare", description = "Compare a target Redis database with a source Redis database and prints the differences")
public class CompareCommand extends AbstractTargetCommand {

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		return jobBuilder.start(verificationStep()).build();
	}

}
