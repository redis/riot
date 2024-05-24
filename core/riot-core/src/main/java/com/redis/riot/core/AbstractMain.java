package com.redis.riot.core;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.expression.Expression;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.Range;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IExecutionStrategy;

@Command(mixinStandardHelpOptions = true)
public class AbstractMain extends BaseCommand implements Runnable {

	PrintWriter out;
	PrintWriter err;

	public PrintWriter getOut() {
		return out;
	}

	public void setOut(PrintWriter out) {
		this.out = out;
	}

	public PrintWriter getErr() {
		return err;
	}

	public void setErr(PrintWriter err) {
		this.err = err;
	}

	@Override
	public void run() {
		commandSpec.commandLine().usage(out);
	}

	public static int run(AbstractMain cmd, String... args) {
		CommandLine commandLine = cmd.commandLine();
		cmd.out = commandLine.getOut();
		cmd.err = commandLine.getErr();
		return execute(commandLine, args, cmd.executionStrategies());
	}

	public static int run(AbstractMain cmd, PrintWriter out, PrintWriter err, String[] args,
			IExecutionStrategy... executionStrategies) {
		CommandLine commandLine = cmd.commandLine();
		commandLine.setOut(out);
		commandLine.setErr(err);
		cmd.out = out;
		cmd.err = err;
		List<IExecutionStrategy> strategies = new ArrayList<>();
		strategies.addAll(Arrays.asList(executionStrategies));
		strategies.addAll(cmd.executionStrategies());
		return execute(commandLine, args, strategies);
	}

	protected List<IExecutionStrategy> executionStrategies() {
		return Collections.emptyList();
	}

	protected CommandLine commandLine() {
		CommandLine commandLine = new CommandLine(this);
		commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
		commandLine.registerConverter(DataSize.class, DataSize::parse);
		commandLine.registerConverter(Range.class, Range::parse);
		commandLine.registerConverter(Expression.class, RiotUtils::parse);
		commandLine.registerConverter(TemplateExpression.class, RiotUtils::parseTemplate);
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		return commandLine;

	}

	private static int execute(CommandLine commandLine, String[] args, List<IExecutionStrategy> executionStrategies) {
		CompositeExecutionStrategy executionStrategy = new CompositeExecutionStrategy(executionStrategies);
		commandLine.setExecutionStrategy(executionStrategy);
		return commandLine.execute(args);
	}

}
