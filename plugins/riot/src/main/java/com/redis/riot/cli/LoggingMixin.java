package com.redis.riot.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.event.Level;

import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Spec.Target;

public class LoggingMixin {

    @Spec(Target.MIXEE)
    private CommandSpec mixee;

    @Option(arity = "1..*", names = "--log", description = "Max log levels (default: ${DEFAULT-VALUE})", paramLabel = "<lvl>")
    Map<String, Level> logs = defaultLogs();

    boolean debug;

    boolean info;

    boolean warn;

    boolean error;

    private static LoggingMixin getTopLevelCommandLoggingMixin(CommandSpec commandSpec) {
        return ((Main) commandSpec.root().userObject()).loggingMixin;
    }

    @Option(names = { "-d", "--debug" }, description = "Log in debug mode (includes normal stacktrace).")
    public void setDebug(boolean debug) {
        getTopLevelCommandLoggingMixin(mixee).debug = debug;
    }

    @Option(names = { "-i", "--info" }, description = "Set log level to info.")
    public void setInfo(boolean info) {
        getTopLevelCommandLoggingMixin(mixee).info = info;
    }

    @Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
    public void setWarn(boolean warn) {
        getTopLevelCommandLoggingMixin(mixee).warn = warn;
    }

    @Option(names = { "-q", "--quiet" }, description = "Log errors only.")
    public void setError(boolean error) {
        getTopLevelCommandLoggingMixin(mixee).error = error;
    }

    public static int executionStrategy(ParseResult parseResult) throws ExecutionException, ParameterException {
        getTopLevelCommandLoggingMixin(parseResult.commandSpec()).configureLogging();
        return ExitCode.OK;
    }

    private static Map<String, Level> defaultLogs() {
        Map<String, Level> logs = new HashMap<>();
        logs.put("org.jline", Level.INFO);
        logs.put("com.amazonaws", Level.ERROR);
        logs.put("io.lettuce", Level.INFO);
        logs.put("io.netty", Level.INFO);
        return logs;
    }

    public void configureLogging() {
        Level logLevel = getTopLevelCommandLoggingMixin(mixee).logLevel();
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel.name());
        for (Entry<String, Level> entry : logs.entrySet()) {
            if (entry.getValue().compareTo(logLevel) < 0) {
                System.setProperty(org.slf4j.impl.SimpleLogger.LOG_KEY_PREFIX + entry.getKey(), entry.getValue().name());
            }
        }
    }

    private Level logLevel() {
        if (debug) {
            return Level.DEBUG;
        }
        if (info) {
            return Level.INFO;
        }
        if (warn) {
            return Level.WARN;
        }
        if (error) {
            return Level.ERROR;
        }
        return Level.WARN;
    }

}
