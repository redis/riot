package com.redislabs.riot.cli.generator;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.generator.FakerGeneratorReader;
import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.generator.SimpleGeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "gen", description = "Import generated data into Redis", subcommands = FakerHelpCommand.class)
public class GeneratorImportCommand extends ImportCommand {

	@Option(names = { "-s", "--simple" }, description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> simpleFields;
	@Option(names = { "-f",
			"--faker" }, description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fakerFields;

	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;

	public GeneratorReader reader() {
		if (isSimple()) {
			return new SimpleGeneratorReader(simpleFields);
		}
		SpelExpressionParser parser = new SpelExpressionParser();
		Map<String, Expression> expressionMap = new LinkedHashMap<String, Expression>();
		fakerFields.forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
		return new FakerGeneratorReader(locale, expressionMap);
	}

	private boolean isSimple() {
		return simpleFields != null;
	}

	@Override
	protected String sourceDescription() {
		if (isSimple()) {
			return "simple gen";
		}
		return "Faker gen";
	}

}
