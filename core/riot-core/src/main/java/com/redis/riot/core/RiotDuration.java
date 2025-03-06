package com.redis.riot.core;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.springframework.boot.convert.DurationStyle;
import org.springframework.util.Assert;

/**
 * Wrapper around java.time.Duration with a custom toString
 */
public class RiotDuration {

	private final Duration value;
	private final ChronoUnit displayUnit;

	public RiotDuration(long value, ChronoUnit unit) {
		this(Duration.of(value, unit), unit);
	}

	public RiotDuration(Duration duration, ChronoUnit displayUnit) {
		Assert.notNull(duration, "Duration must not be null");
		Assert.notNull(displayUnit, "Unit must not be null");
		this.value = duration;
		this.displayUnit = displayUnit;
	}

	public Duration getValue() {
		return value;
	}

	@Override
	public String toString() {
		return DurationStyle.SIMPLE.print(value, displayUnit);
	}

	public static RiotDuration parse(String string) {
		return new RiotDuration(DurationStyle.SIMPLE.parse(string), ChronoUnit.MILLIS);
	}

	public static RiotDuration of(long value, ChronoUnit unit) {
		return new RiotDuration(value, unit);
	}

	public static RiotDuration of(Duration duration, ChronoUnit unit) {
		return new RiotDuration(duration, unit);
	}

	public static RiotDuration ofSeconds(long seconds) {
		return new RiotDuration(seconds, ChronoUnit.SECONDS);
	}

	public static RiotDuration ofMillis(long millis) {
		return new RiotDuration(millis, ChronoUnit.MILLIS);
	}
}
