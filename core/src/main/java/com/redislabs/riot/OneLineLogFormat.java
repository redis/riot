package com.redislabs.riot;

import org.springframework.core.NestedExceptionUtils;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OneLineLogFormat extends Formatter {

    @Override
    public String format(LogRecord record) {
        String message = formatMessage(record);
        if (record.getThrown() != null) {
            Throwable rootCause = NestedExceptionUtils.getRootCause(record.getThrown());
            if (rootCause != null && rootCause.getMessage() != null) {
                return String.format("%s: %s%n", message, rootCause.getMessage());
            }
        }
        return String.format("%s%n", message);
    }

}