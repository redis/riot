package com.redis.riot.cli;

import org.slf4j.event.Level;

import picocli.CommandLine.Option;

public class LoggingArgs {

    @Option(names = { "-d", "--debug" }, description = "Log in debug mode (includes normal stacktrace).")
    public void setDebug(boolean debug) {
        if (debug) {
            setLogLevel(Level.DEBUG);
        }
    }

    @Option(names = { "-i", "--info" }, description = "Set log level to info.")
    public void setInfo(boolean info) {
        if (info) {
            setLogLevel(Level.INFO);
        }
    }

    @Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
    public void setWarn(boolean warn) {
        if (warn) {
            setLogLevel(Level.WARN);
        }
    }

    @Option(names = { "-q", "--quiet" }, description = "Log errors only.")
    public void setQuiet(boolean quiet) {
        if (quiet) {
            setLogLevel(Level.ERROR);
        }
    }

    private void setLogLevel(Level level) {
        setLogLevel(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, level);
    }

    public static void setLogLevel(String key, Level level) {
        System.setProperty(key, level.name());
    }

}
