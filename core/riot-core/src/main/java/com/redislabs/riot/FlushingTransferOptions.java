package com.redislabs.riot;

import lombok.Data;
import picocli.CommandLine;

import java.time.Duration;

@Data
public class FlushingTransferOptions {

    @CommandLine.Option(names = "--flush-interval", description = "Max duration between flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushInterval = 50;
    @CommandLine.Option(names = "--idle-timeout", description = "Min duration of inactivity to consider transfer complete", paramLabel = "<ms>")
    private Long idleTimeout;

    public Duration getFlushIntervalDuration() {
        return Duration.ofMillis(flushInterval);
    }

    public void setIdleTimeout(Duration idleTimeoutDuration) {
        if (idleTimeoutDuration == null) {
            this.idleTimeout = null;
        } else {
            this.idleTimeout = idleTimeoutDuration.toMillis();
        }
    }

    public Duration getIdleTimeoutDuration() {
        if (idleTimeout == null) {
            return null;
        }
        return Duration.ofMillis(idleTimeout);
    }

}
