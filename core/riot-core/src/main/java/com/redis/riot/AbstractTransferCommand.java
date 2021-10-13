package com.redis.riot;

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

}
