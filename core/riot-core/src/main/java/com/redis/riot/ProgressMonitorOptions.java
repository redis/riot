package com.redis.riot;

import java.time.Duration;

import com.redis.riot.ProgressMonitor.Builder;
import com.redis.riot.ProgressMonitor.Style;

import picocli.CommandLine.Option;

public class ProgressMonitorOptions {

	@Option(names = "--progress", description = "Style of progress bar: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}),", paramLabel = "<style>")
	private Style style = Style.COLOR;
	@Option(names = "--progress-interval", description = "Progress update interval in milliseconds (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	private long updateIntervalMillis = 300;

	public Style getStyle() {
		return style;
	}

	public long getUpdateIntervalMillis() {
		return updateIntervalMillis;
	}

	public void setStyle(Style style) {
		this.style = style;
	}

	public void setUpdateIntervalMillis(long progressUpdateIntervalMillis) {
		this.updateIntervalMillis = progressUpdateIntervalMillis;
	}

	public Builder monitor() {
		return ProgressMonitor.style(style).updateInterval(Duration.ofMillis(updateIntervalMillis));
	}

	public boolean isEnabled() {
		return getStyle() != Style.NONE;
	}
}
