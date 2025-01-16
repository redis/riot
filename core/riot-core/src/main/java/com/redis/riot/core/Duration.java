package com.redis.riot.core;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.util.Assert;

import static java.lang.Math.floor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Duration implements Comparable<Duration> {

	private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");

	// We iterate over the TIME_UNITS constant in convertToMostSuccinctTimeUnit()
	// instead of TimeUnit.values() as the latter results in non-trivial amount of
	// memory
	// allocation when that method is called in a tight loop. The reason is that the
	// values()
	// call allocates a new array at each call.
	private static final TimeUnit[] TIME_UNITS = TimeUnit.values();

	public static final Duration ZERO = new Duration(0, SECONDS);

	public static Duration nanosSince(long start) {
		return succinctNanos(System.nanoTime() - start);
	}

	public static Duration succinctNanos(long nanos) {
		return succinctDuration(nanos, NANOSECONDS);
	}

	public static Duration succinctDuration(double value, TimeUnit unit) {
		if (value == 0) {
			return ZERO;
		}
		return new Duration(value, unit).convertToMostSuccinctTimeUnit();
	}

	private final double value;
	private final TimeUnit unit;

	public Duration(double value, TimeUnit unit) {
		if (Double.isInfinite(value)) {
			throw new IllegalArgumentException("value is infinite: " + value);
		}
		Assert.isTrue(!Double.isNaN(value), "value is not a number");
		if (value < 0) {
			throw new IllegalArgumentException("value is negative: " + value);
		}
		requireNonNull(unit, "unit is null");

		this.value = value;
		this.unit = unit;
	}

	public long toMillis() {
		return roundTo(MILLISECONDS);
	}

	public double getValue() {
		return value;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public double getValue(TimeUnit timeUnit) {
		requireNonNull(timeUnit, "timeUnit is null");
		return value * (millisPerTimeUnit(this.unit) * 1.0 / millisPerTimeUnit(timeUnit));
	}

	public long roundTo(TimeUnit timeUnit) {
		requireNonNull(timeUnit, "timeUnit is null");
		double rounded = Math.floor(getValue(timeUnit) + 0.5d);
		if (rounded > Long.MAX_VALUE) {
			throw new IllegalArgumentException(
					format("value %s %s is too large to be represented in requested unit %s as a long", value, unit,
							timeUnit));
		}
		return (long) rounded;
	}

	public Duration convertTo(TimeUnit timeUnit) {
		requireNonNull(timeUnit, "timeUnit is null");
		return new Duration(getValue(timeUnit), timeUnit);
	}

	public Duration convertToMostSuccinctTimeUnit() {
		TimeUnit unitToUse = NANOSECONDS;
		for (TimeUnit unitToTest : TIME_UNITS) {
			// since time units are powers of ten, we can get rounding errors here, so fuzzy
			// match
			if (getValue(unitToTest) > 0.9999) {
				unitToUse = unitToTest;
			} else {
				break;
			}
		}
		return convertTo(unitToUse);
	}

	public java.time.Duration toJavaTime() {
		long seconds;
		long nanoAdjustment;
		long secondsPerUnit = SECONDS.convert(1, unit);
		long nanosPerUnit = NANOSECONDS.convert(1, unit);
		if (secondsPerUnit > 1) {
			seconds = (long) floor(value * secondsPerUnit);
			nanoAdjustment = (long) floor((value - (double) seconds / secondsPerUnit) * nanosPerUnit);
		} else {
			long unitsPerSecond = unit.convert(1, SECONDS);
			seconds = (long) floor(value / unitsPerSecond);
			nanoAdjustment = (long) floor((value - (double) seconds * unitsPerSecond) * nanosPerUnit);
		}

		if (seconds == Long.MAX_VALUE) {
			nanoAdjustment = 0;
		}
		return java.time.Duration.ofSeconds(seconds, nanoAdjustment);
	}

	@Override
	public String toString() {
		return toString(unit);
	}

	public String toString(TimeUnit timeUnit) {
		requireNonNull(timeUnit, "timeUnit is null");
		double magnitude = getValue(timeUnit);
		String timeUnitAbbreviation = timeUnitToString(timeUnit);
		return format(Locale.ENGLISH, "%.2f%s", magnitude, timeUnitAbbreviation);
	}

	public static Duration parse(String string) throws IllegalArgumentException {
		requireNonNull(string, "duration is null");
		Assert.isTrue(!string.isEmpty(), "duration is empty");

		Matcher matcher = PATTERN.matcher(string);
		if (!matcher.matches()) {
			throw new IllegalArgumentException("duration is not a valid data duration string: " + string);
		}

		double value = Double.parseDouble(matcher.group(1));
		String unitString = matcher.group(2);

		TimeUnit timeUnit = valueOfTimeUnit(unitString);
		return new Duration(value, timeUnit);
	}

	@Override
	public int compareTo(Duration o) {
		return Double.compare(getValue(MILLISECONDS), o.getValue(MILLISECONDS));
	}

	public boolean isZero() {
		return equals(ZERO);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Duration duration = (Duration) o;

		return compareTo(duration) == 0;
	}

	@Override
	public int hashCode() {
		double value = getValue(MILLISECONDS);
		return Double.hashCode(value);
	}

	public static TimeUnit valueOfTimeUnit(String timeUnitString) {
		requireNonNull(timeUnitString, "timeUnitString is null");
		switch (timeUnitString) {
		case "ns":
			return NANOSECONDS;
		case "us":
			return MICROSECONDS;
		case "ms":
			return MILLISECONDS;
		case "s":
			return SECONDS;
		case "m":
			return MINUTES;
		case "h":
			return HOURS;
		case "d":
			return DAYS;
		default:
			throw new IllegalArgumentException("Unknown time unit: " + timeUnitString);
		}
	}

	public static String timeUnitToString(TimeUnit timeUnit) {
		requireNonNull(timeUnit, "timeUnit is null");
		switch (timeUnit) {
		case NANOSECONDS:
			return "ns";
		case MICROSECONDS:
			return "us";
		case MILLISECONDS:
			return "ms";
		case SECONDS:
			return "s";
		case MINUTES:
			return "m";
		case HOURS:
			return "h";
		case DAYS:
			return "d";
		default:
			throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
		}
	}

	private static double millisPerTimeUnit(TimeUnit timeUnit) {
		switch (timeUnit) {
		case NANOSECONDS:
			return 1.0 / 1000000.0;
		case MICROSECONDS:
			return 1.0 / 1000.0;
		case MILLISECONDS:
			return 1;
		case SECONDS:
			return 1000;
		case MINUTES:
			return 1000 * 60;
		case HOURS:
			return 1000 * 60 * 60;
		case DAYS:
			return 1000 * 60 * 60 * 24;
		default:
			throw new IllegalArgumentException("Unsupported time unit " + timeUnit);
		}
	}
}