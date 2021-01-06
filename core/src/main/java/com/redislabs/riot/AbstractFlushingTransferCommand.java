package com.redislabs.riot;

import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import picocli.CommandLine;

import java.time.Duration;

public abstract class AbstractFlushingTransferCommand<I, O> extends AbstractTransferCommand<I, O> {

    @CommandLine.Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushInterval = 50;

    @Override
    protected FlushingStepBuilder<I, O> simpleStep(String name) {
        Duration timeout = Duration.ofMillis(flushInterval);
        return new FlushingStepBuilder<I, O>(super.simpleStep(name)).timeout(timeout);
    }

}
