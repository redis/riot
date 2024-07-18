package com.redis.riot.core;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

public class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

	public int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {

		boolean stacktrace = false;

		if (cmd.getCommand() instanceof AbstractCommand) {
			stacktrace = ((AbstractCommand) cmd.getCommand()).getLoggingArgs().isStacktrace();
		}

		if (stacktrace) {
			ex.printStackTrace(cmd.getErr());
		}

		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));

		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}
}