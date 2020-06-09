package com.redislabs.riot.cli.progress;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class ProgressBarOptions {
    private Integer initialMax;
    @NonNull
    private String taskName;
    @Builder.Default
    private long refreshInterval = 300;
    private boolean quiet;
}