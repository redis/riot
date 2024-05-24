package com.redis.riot.core;

import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

class PrintExceptionMessageHandler implements IExecutionExceptionHandler {

	public int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {

		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));

		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}
}