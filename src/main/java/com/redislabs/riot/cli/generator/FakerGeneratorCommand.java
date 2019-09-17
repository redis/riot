package com.redislabs.riot.cli.generator;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.github.javafaker.Faker;
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.generator.FakerGeneratorReader;
import com.redislabs.riot.generator.GeneratorReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "faker", description = "Import Faker-generated data")
public class FakerGeneratorCommand extends ImportCommand {

	@Option(required = true, arity = "1..*", names = "--fields", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fields;
	@Option(names = { "-l",
			"--locale" }, description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = { "--faker-help" }, description = "Show all available Faker properties")
	private boolean fakerHelp;

	@Override
	protected GeneratorReader reader() {
		SpelExpressionParser parser = new SpelExpressionParser();
		Map<String, Expression> expressionMap = new LinkedHashMap<String, Expression>();
		fields.forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
		return new FakerGeneratorReader(locale, expressionMap);
	}

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@Override
	public void run() {
		if (fakerHelp) {
			Arrays.asList(Faker.class.getDeclaredMethods()).stream().filter(this::accept)
					.sorted((m1, m2) -> m1.getName().compareTo(m2.getName())).forEach(m -> describe(m));
		} else {
			super.run();
		}
	}

	private boolean accept(Method method) {
		if (EXCLUDES.contains(method.getName())) {
			return false;
		}
		return method.getReturnType().getPackage().equals(Faker.class.getPackage());
	}

	private void describe(Method method) {
		System.out.println(method.getName());
		Arrays.asList(method.getReturnType().getDeclaredMethods()).stream().filter(m -> m.getParameters().length == 0)
				.map(m -> m.getName()).forEach(n -> System.out.println(" ." + n));
	}

}
