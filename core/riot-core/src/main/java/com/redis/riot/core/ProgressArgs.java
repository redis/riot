package com.redis.riot.core;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class ProgressArgs {

	public static final RiotDuration DEFAULT_UPDATE_INTERVAL = RiotDuration.ofSeconds(1);
	public static final ProgressStyle DEFAULT_STYLE = ProgressStyle.ASCII;

	@Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
	private ProgressStyle style = DEFAULT_STYLE;

	@Option(names = "--progress-rate", description = "Progress update interval (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>", hidden = true)
	private RiotDuration updateInterval = DEFAULT_UPDATE_INTERVAL;

	public ProgressStyle getStyle() {
		return style;
	}

	public void setStyle(ProgressStyle style) {
		this.style = style;
	}

	public RiotDuration getUpdateInterval() {
		return updateInterval;
	}

	public void setUpdateInterval(RiotDuration interval) {
		this.updateInterval = interval;
	}

}
