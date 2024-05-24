package com.redis.riot.core;

import picocli.CommandLine.Option;

public class ProgressArgs {

	public static final long DEFAULT_UPDATE_INTERVAL = 1000;
	public static final ProgressStyle DEFAULT_STYLE = ProgressStyle.ASCII;

	@Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
	private ProgressStyle style = DEFAULT_STYLE;

	@Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	private long updateInterval = DEFAULT_UPDATE_INTERVAL;

	public ProgressStyle getStyle() {
		return style;
	}

	public void setStyle(ProgressStyle style) {
		this.style = style;
	}

	public long getUpdateInterval() {
		return updateInterval;
	}

	public void setUpdateInterval(long interval) {
		this.updateInterval = interval;
	}

}
