package com.redis.riot;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.core.step.builder.StepBuilder;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractTransferCommand extends AbstractRiotCommand {

    @CommandLine.Mixin
    private TransferOptions transferOptions = new TransferOptions();

    protected <I, O> RiotStepBuilder<I, O> riotStep(StepBuilder stepBuilder, String taskName) {
        return new RiotStepBuilder<I, O>(stepBuilder, transferOptions).taskName(taskName);
    }
    
	protected <T> GenericObjectPoolConfig<T> poolConfig(int poolMaxTotal) {
		GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(poolMaxTotal);
		return config;
	}

}
