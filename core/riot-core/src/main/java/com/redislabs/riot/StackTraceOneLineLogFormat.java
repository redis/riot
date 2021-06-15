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

public class StackTraceOneLineLogFormat extends Formatter {

    private final DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();
    private final ZoneId offset = ZoneOffset.systemDefault();

    @Override
    public String format(LogRecord record) {
        String message = formatMessage(record);
        ZonedDateTime time = Instant.ofEpochMilli(record.getMillis()).atZone(offset);
        if (record.getThrown() == null) {
            return String.format("%s %s %s\t: %s%n", time.format(d), record.getLevel().getLocalizedName(), record.getLoggerName(), message);
        }
        return String.format("%s %s %s\t: %s%n%s%n", time.format(d), record.getLevel().getLocalizedName(), record.getLoggerName(), message, stackTrace(record));
    }

    private String stackTrace(LogRecord record) {
        StringWriter sw = new StringWriter(4096);
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        return sw.toString();
    }
}
