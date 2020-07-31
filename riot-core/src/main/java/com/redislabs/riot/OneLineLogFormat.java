package com.redislabs.riot;

import org.apache.commons.lang3.exception.ExceptionUtils;

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

    private final DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();
    private final ZoneId offset = ZoneOffset.systemDefault();
    private final boolean verbose;

    public OneLineLogFormat(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public String format(LogRecord record) {
        String message = formatMessage(record);
        ZonedDateTime time = Instant.ofEpochMilli(record.getMillis()).atZone(offset);
        if (record.getThrown() == null) {
            if (verbose) {
                return String.format("%s %s %s\t: %s%n", time.format(d), record.getLevel().getLocalizedName(), record.getLoggerName(), message);
            }
            return String.format("%s%n", message);
        }
        if (verbose) {
            return String.format("%s %s %s\t: %s%n%s%n", time.format(d), record.getLevel().getLocalizedName(), record.getLoggerName(), message, stackTrace(record));
        }
        Throwable rootCause = ExceptionUtils.getRootCause(record.getThrown());
        if (rootCause == null || rootCause.getMessage() == null) {
            return String.format("%s%n", message);
        }
        return String.format("%s: %s%n", message, rootCause.getMessage());
    }

    private String stackTrace(LogRecord record) {
        StringWriter sw = new StringWriter(4096);
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        return sw.toString();
    }
}
