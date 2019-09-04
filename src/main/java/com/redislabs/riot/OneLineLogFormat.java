package com.redislabs.riot;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OneLineLogFormat extends Formatter {

	private DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
			.appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':')
			.appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart()
			.appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();
	private ZoneId offset = ZoneOffset.systemDefault();
	private boolean verbose;

	public OneLineLogFormat(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public String format(LogRecord record) {
		String message = formatMessage(record);
		ZonedDateTime time = Instant.ofEpochMilli(record.getMillis()).atZone(offset);
		if (record.getThrown() == null) {
			return String.format("%s\t%s%n", time.format(d), message);
		}
		if (verbose) {
			return String.format("%s\t%s%n%s%n", time.format(d), message, stackTrace(record));
		}
		return String.format("%s\t%s: %s%n", time.format(d), message, record.getThrown().getMessage());
	}

	private String stackTrace(LogRecord record) {
		StringWriter sw = new StringWriter(4096);
		PrintWriter pw = new PrintWriter(sw);
		record.getThrown().printStackTrace(pw);
		return sw.toString();
	}
}
