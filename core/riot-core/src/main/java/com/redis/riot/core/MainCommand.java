package com.redis.riot.core;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

import org.springframework.util.unit.DataSize;

import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;

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
		registerConverters(commandLine);
		commandLine.setExecutionStrategy(
				new CompositeExecutionStrategy(LoggingMixin::executionStrategy, this::executionStrategy));
		return commandLine.execute(args);
	}

	protected int executionStrategy(ParseResult parseResult) {
		return new RunLast().execute(parseResult);
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RiotDuration.class, RiotDuration::parse);
		commandLine.registerConverter(DataSize.class, MainCommand::parseDataSize);
		commandLine.registerConverter(Expression.class, Expression::parse);
		commandLine.registerConverter(TemplateExpression.class, Expression::parseTemplate);
	}

	public static DataSize parseDataSize(String string) {
		return DataSize.parse(string.toUpperCase());
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
