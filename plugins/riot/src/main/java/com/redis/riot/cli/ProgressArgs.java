package com.redis.riot.cli;

import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.Option;

public class ProgressArgs {

    public enum Style {
        BLOCK, BAR, ASCII, LOG, NONE
    }

    @Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
    Style style = Style.ASCII;

    @Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
    int updateInterval = 300;

    public ProgressBarStyle style() {
        switch (style) {
            case BAR:
                return ProgressBarStyle.COLORFUL_UNICODE_BAR;
            case BLOCK:
                return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
            default:
                return ProgressBarStyle.ASCII;
        }
    }

    public boolean isLog() {
        return style == Style.LOG;
    }

}
