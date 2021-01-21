package com.redislabs.riot;

import picocli.CommandLine;

public abstract class AbstractTransferCommand<I, O> extends AbstractTaskCommand {

    @CommandLine.Mixin
    private TransferOptions transferOptions = TransferOptions.builder().build();

    protected <S,T> StepBuilder<S, T> stepBuilder(String name) {
        return new StepBuilder<S, T>(jobFactory, transferOptions).name(name);
    }


}
