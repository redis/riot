package com.redislabs.riot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.redis.support.FlushingStepBuilder;
import picocli.CommandLine;

import java.time.Duration;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FlushingTransferOptions {

    @Builder.Default
    @CommandLine.Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    private long flushInterval = 50;
    @CommandLine.Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete.", paramLabel = "<ms>")
    private Long idleTimeout;

    public Duration getFlushIntervalDuration() {
        return Duration.ofMillis(flushInterval);
    }

    public Duration getIdleTimeoutDuration() {
        if (idleTimeout == null) {
            return null;
        }
        return Duration.ofMillis(idleTimeout);
    }

    public <S, T> FlushingStepBuilder<S, T> configure(SimpleStepBuilder<S, T> step) {
        FlushingStepBuilder<S, T> builder = new FlushingStepBuilder<>(step).flushingInterval(getFlushIntervalDuration());
        Duration idleTimeoutDuration = getIdleTimeoutDuration();
        if (idleTimeoutDuration != null) {
            builder.idleTimeout(idleTimeoutDuration);
        }
        return builder;
    }

}
