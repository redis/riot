package com.redis.riot.redis;

import org.springframework.batch.core.job.flow.Flow;

import com.redis.spring.batch.support.KeyValue;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@CommandLine.Command(name = "compare", description = "Compare a target Redis database with a source Redis database and prints the differences")
public class CompareCommand<T extends KeyValue<String, ?>> extends AbstractTargetCommand {

	@Override
	protected Flow flow() throws Exception {
		return verificationFlow();
	}

}
