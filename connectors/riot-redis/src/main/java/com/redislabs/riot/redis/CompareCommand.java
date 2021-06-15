package com.redislabs.riot.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.redis.support.KeyValue;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
@CommandLine.Command(name = "compare", description = "Compare a target Redis database with a source Redis database and prints the differences")
public class CompareCommand<T extends KeyValue<String, ?>> extends AbstractTargetCommand {

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) {
        return verificationFlow(stepBuilderFactory);
    }

}
