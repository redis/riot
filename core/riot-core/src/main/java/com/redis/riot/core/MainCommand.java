package com.redis.riot.core;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

import org.springframework.util.unit.DataSize;

import picocli.CommandLine;

public class MainCommand extends BaseCommand implements Callable<Integer>, IO {

	private PrintWriter out;
	private PrintWriter err;

	@Override
	public Integer call() throws Exception {
		commandSpec.commandLine().usage(out);
		return 0;
	}

	protected CommandLine commandLine() {
		return new CommandLine(this);
	}

	public int run(String... args) {
		CommandLine commandLine = commandLine();
		setOut(commandLine.getOut());
		setErr(commandLine.getErr());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
		commandLine.registerConverter(DataSize.class, DataSize::parse);
		commandLine.registerConverter(Expression.class, Expression::parse);
		commandLine.registerConverter(Duration.class, Duration::parse);
		commandLine.registerConverter(TemplateExpression.class, Expression::parseTemplate);
		commandLine.setExecutionStrategy(LoggingMixin.executionStrategy(commandLine.getExecutionStrategy()));
		return commandLine.execute(args);
	}

	@Override
	public PrintWriter getOut() {
		return out;
	}

	@Override
	public void setOut(PrintWriter out) {
		this.out = out;
	}

	@Override
	public PrintWriter getErr() {
		return err;
	}

	@Override
	public void setErr(PrintWriter err) {
		this.err = err;
	}

}
