package com.redis.riot;

import java.util.logging.Level;

import picocli.CommandLine.Option;

public class LoggingOptions {

	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	private boolean quiet;
	@Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
	private boolean warning;
	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	private boolean info;
	@Option(names = { "-d", "--debug" }, description = "Log in debug mode (includes normal stacktrace).")
	private boolean debug;
	@Option(names = "--stacktrace", description = "Print out the stacktrace for all exceptions.")
	private boolean stacktrace;

	public boolean isQuiet() {
		return quiet;
	}

	public void setQuiet(boolean quiet) {
		this.quiet = quiet;
	}

	public boolean isWarning() {
		return warning;
	}

	public void setWarning(boolean warning) {
		this.warning = warning;
	}

	public boolean isInfo() {
		return info;
	}

	public void setInfo(boolean info) {
		this.info = info;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public boolean isStacktrace() {
		return stacktrace || debug;
	}

	public void setStacktrace(boolean stacktrace) {
		this.stacktrace = stacktrace;
	}

	public Level getLevel() {
		if (debug) {
			return Level.FINE;
		}
		if (info) {
			return Level.INFO;
		}
		if (warning) {
			return Level.SEVERE;
		}
		if (quiet) {
			return Level.OFF;
		}
		return Level.WARNING;
	}

	public Level getRiotLevel() {
		if (debug) {
			return Level.FINER;
		}
		if (info) {
			return Level.FINE;
		}
		if (warning) {
			return Level.WARNING;
		}
		if (quiet) {
			return Level.SEVERE;
		}
		return Level.INFO;
	}

}
