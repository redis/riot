package com.redis.riot.core;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

public class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

	public int handleExecutionException(Exception exception, CommandLine cmd, ParseResult parseResult) {

		Throwable finalException = unwrapException(exception);

		if (cmd.getCommand() instanceof BaseCommand) {
			if (((BaseCommand) cmd.getCommand()).loggingMixin.isStacktrace()) {
				finalException.printStackTrace(cmd.getErr());
			}
		}

		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(finalException.getMessage()));

		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(finalException)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}

	private Throwable unwrapException(Exception exception) {
		if (exception instanceof RiotException) {
			RiotException riotException = (RiotException) exception;
			if (riotException.getCause() == null) {
				return riotException;
			}
			return riotException.getCause();
		}
		return exception;
	}

}