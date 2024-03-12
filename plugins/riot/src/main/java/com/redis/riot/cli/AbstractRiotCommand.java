package com.redis.riot.cli;

import java.util.Map;

import org.springframework.expression.Expression;

import com.redis.riot.core.AbstractRiotRunnable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractRiotCommand extends BaseCommand implements Runnable {

	@ParentCommand
	protected Main parent;

	@Option(arity = "1..*", names = "--var", description = "SpEL expressions for context variables, in the form var=\"exp\"", paramLabel = "<v=exp>")
	Map<String, Expression> variableExpressions;

	@Option(names = "--date-format", description = "Date/time format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-simple-date-format#date_time_patterns", paramLabel = "<fmt>")
	String dateFormat = AbstractRiotRunnable.DEFAULT_DATE_FORMAT;

	@Override
	public void run() {
		AbstractRiotRunnable executable = executable();
		executable.setRedisOptions(parent.redisArgs.redisOptions());
		executable.setVarExpressions(variableExpressions);
		executable.setDateFormat(dateFormat);
		executable.run();
	}

	protected abstract AbstractRiotRunnable executable();

}
