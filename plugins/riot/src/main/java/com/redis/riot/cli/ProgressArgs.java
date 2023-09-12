package com.redis.riot.cli;

import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.Option;

public class ProgressArgs {

    public enum ProgressStyle {
        BLOCK, BAR, ASCII, LOG, NONE
    }

    @Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
    ProgressStyle style = ProgressStyle.ASCII;

    @Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
    int updateInterval = 300;

    public ProgressBarStyle progressBarStyle() {
        switch (style) {
            case BAR:
                return ProgressBarStyle.COLORFUL_UNICODE_BAR;
            case BLOCK:
                return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
            default:
                return ProgressBarStyle.ASCII;
        }
    }

}
