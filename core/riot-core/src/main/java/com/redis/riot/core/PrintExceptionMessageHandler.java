package com.redis.riot.core;

import java.util.concurrent.ExecutionException;

import org.springframework.batch.core.step.skip.SkipException;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

public class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

	public int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {

		boolean stacktrace = false;

		if (cmd.getCommand() instanceof AbstractCommand) {
			stacktrace = ((AbstractCommand<?>) cmd.getCommand()).getLoggingArgs().isStacktrace();
		}

		if (stacktrace) {
			ex.printStackTrace(cmd.getErr());
		}

		Throwable rootCause = rootCause(ex);
		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(rootCause.getMessage()));

		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(rootCause)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}

	private Throwable rootCause(Throwable ex) {
		if (ex instanceof SkipException) {
			return rootCause(((SkipException) ex).getCause());
		}
		if (ex instanceof ExecutionException) {
			return ((ExecutionException) ex).getCause();
		}
		return ex;
	}
}