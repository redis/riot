package com.redis.riot.cli;

import org.slf4j.event.Level;

import picocli.CommandLine.Option;

public class LoggingArgs {

    @Option(names = { "-d", "--debug" })
    private boolean debug;

    @Option(names = { "-i", "--info" })
    private boolean info;

    @Option(names = { "-w", "--warn" })
    private boolean warn;

    @Option(names = { "-q", "--quiet" })
    private boolean quiet;

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isInfo() {
        return info;
    }

    public void setInfo(boolean info) {
        this.info = info;
    }

    public boolean isWarn() {
        return warn;
    }

    public void setWarn(boolean warn) {
        this.warn = warn;
    }

    public boolean isQuiet() {
        return quiet;
    }

    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }

    public Level getLevel() {
        if (debug) {
            return Level.DEBUG;
        }
        if (info) {
            return Level.INFO;
        }
        if (warn) {
            return Level.WARN;
        }
        if (quiet) {
            return Level.ERROR;
        }
        return null;
    }

}
