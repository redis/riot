package com.redis.riot.redis;

import org.springframework.batch.core.job.flow.Flow;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine.Command;

@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "compare", description = "Compare a target Redis database with a source Redis database and prints the differences")
public class CompareCommand extends AbstractTargetCommand {

	@Override
	protected Flow flow() throws Exception {
		return verificationFlow();
	}

}
