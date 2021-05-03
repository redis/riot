package com.redislabs.riot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import picocli.CommandLine;

import java.time.Duration;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractFlushingTransferCommand extends AbstractTransferCommand {

    @CommandLine.Mixin
    protected FlushingTransferOptions flushingTransferOptions = FlushingTransferOptions.builder().build();

    public <S, T> FlushingStepBuilder<S, T> configure(SimpleStepBuilder<S, T> step) {
        log.info("Configuring flushing transfer with {}", flushingTransferOptions);
        FlushingStepBuilder<S, T> builder = new FlushingStepBuilder<>(step).flushingInterval(flushingTransferOptions.getFlushIntervalDuration());
        Duration idleTimeoutDuration = flushingTransferOptions.getIdleTimeoutDuration();
        if (idleTimeoutDuration != null) {
            builder.idleTimeout(idleTimeoutDuration);
        }
        return builder;
    }
}
