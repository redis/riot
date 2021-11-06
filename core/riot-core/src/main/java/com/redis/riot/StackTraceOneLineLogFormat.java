package com.redis.riot;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class StackTraceOneLineLogFormat extends Formatter {

	private final DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
			.appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':')
			.appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();
	
	private final ZoneId offset = TimeZone.getDefault().toZoneId();

	@Override
	public String format(LogRecord logRecord) {
		String message = formatMessage(logRecord);
		ZonedDateTime time = Instant.ofEpochMilli(logRecord.getMillis()).atZone(offset);
		if (logRecord.getThrown() == null) {
			return String.format("%s %s %s\t: %s%n", time.format(d), logRecord.getLevel().getLocalizedName(),
					logRecord.getLoggerName(), message);
		}
		return String.format("%s %s %s\t: %s%n%s%n", time.format(d), logRecord.getLevel().getLocalizedName(),
				logRecord.getLoggerName(), message, stackTrace(logRecord));
	}

	private String stackTrace(LogRecord logRecord) {
		StringWriter sw = new StringWriter(4096);
		PrintWriter pw = new PrintWriter(sw);
		logRecord.getThrown().printStackTrace(pw);
		return sw.toString();
	}
}
