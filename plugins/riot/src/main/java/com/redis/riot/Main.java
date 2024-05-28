package com.redis.riot;

import java.io.PrintWriter;

import org.springframework.expression.Expression;
import org.springframework.util.unit.DataSize;

import com.redis.riot.core.BaseCommand;
import com.redis.riot.core.IO;
import com.redis.riot.core.PrintExceptionMessageHandler;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.operation.OperationCommand;
import com.redis.spring.batch.Range;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunFirst;
import picocli.CommandLine.RunLast;

@Command(name = "riot", versionProvider = Versions.class, headerHeading = "RIOT is a data import/export tool for Redis.%n%n", footerHeading = "%nDocumentation found at http://redis.github.io/riot%n", mixinStandardHelpOptions = true, subcommands = {
		DatabaseImport.class, DatabaseExport.class, FileImport.class, FileExport.class, FakerImport.class,
		Generate.class, Replicate.class, Compare.class, Ping.class, GenerateCompletion.class })
public class Main extends BaseCommand implements Runnable, IO {

	private PrintWriter out;
	private PrintWriter err;

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

	@Override
	public void run() {
		commandSpec.commandLine().usage(out);
	}

	public static CommandLine commandLine(IO main) {
		CommandLine commandLine = new CommandLine(main);
		main.setOut(commandLine.getOut());
		main.setErr(commandLine.getErr());
		return commandLine;
	}

	public static int run(CommandLine commandLine, String... args) {
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		commandLine.setExecutionExceptionHandler(new PrintExceptionMessageHandler());
		commandLine.setUsageHelpWidth(110);
		commandLine.setUsageHelpLongOptionsMaxWidth(42);
		commandLine.registerConverter(DataSize.class, DataSize::parse);
		commandLine.registerConverter(Range.class, Range::parse);
		commandLine.registerConverter(Expression.class, RiotUtils::parse);
		commandLine.registerConverter(TemplateExpression.class, RiotUtils::parseTemplate);
		return commandLine.execute(args);
	}

	public static void main(String[] args) {
		CommandLine commandLine = commandLine(new Main());
		commandLine.setExecutionStrategy(Main::executionStrategy);
		System.exit(run(commandLine, args));
	}

	public static int executionStrategy(ParseResult parseResult) {
		for (ParseResult subcommand : parseResult.subcommands()) {
			Object command = subcommand.commandSpec().userObject();
			if (AbstractImport.class.isAssignableFrom(command.getClass())) {
				AbstractImport importCommand = (AbstractImport) command;
				for (ParseResult redisCommand : subcommand.subcommands()) {
					if (redisCommand.isUsageHelpRequested()) {
						return new RunLast().execute(redisCommand);
					}
					importCommand.getImportOperationCommands()
							.add((OperationCommand) redisCommand.commandSpec().userObject());
				}
				return new RunFirst().execute(subcommand);
			}
		}
		return new RunLast().execute(parseResult); // default execution strategy
	}
}
