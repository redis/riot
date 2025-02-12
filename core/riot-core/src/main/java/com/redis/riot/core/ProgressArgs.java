package com.redis.riot.core;

import java.time.Duration;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class ProgressArgs {

	public static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofSeconds(1);
	public static final ProgressStyle DEFAULT_STYLE = ProgressStyle.ASCII;

	@Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
	private ProgressStyle style = DEFAULT_STYLE;

	@Option(names = "--progress-rate", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<dur>", hidden = true)
	private Duration updateInterval = DEFAULT_UPDATE_INTERVAL;

	public ProgressStyle getStyle() {
		return style;
	}

	public void setStyle(ProgressStyle style) {
		this.style = style;
	}

	public Duration getUpdateInterval() {
		return updateInterval;
	}

	public void setUpdateInterval(Duration interval) {
		this.updateInterval = interval;
	}

}
