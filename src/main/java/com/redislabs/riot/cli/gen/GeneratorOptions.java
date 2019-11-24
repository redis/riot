package com.redislabs.riot.cli.gen;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.github.javafaker.Faker;
import com.redislabs.riot.batch.generator.GeneratorReader;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class GeneratorOptions {

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@Option(arity = "1..*", names = "--faker-fields", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fakerFields = new LinkedHashMap<>();
	@Option(names = "--simple-fields", arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> simpleFields = new LinkedHashMap<>();
	@Option(names = { "-l",
			"--locale" }, description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = { "--faker-help" }, description = "Show all available Faker properties")
	private boolean fakerHelp;

	/*
	 * TODO move to gen command
	 */
	public void run() {
		if (fakerHelp) {
			showFakerHelp();
			return;
		}
	}

	public GeneratorReader reader() {
		SpelExpressionParser parser = new SpelExpressionParser();
		Map<String, Expression> expressionMap = new LinkedHashMap<String, Expression>();
		fakerFields.forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
		return new GeneratorReader(locale, expressionMap, simpleFields);
	}

	private boolean accept(Method method) {
		if (EXCLUDES.contains(method.getName())) {
			return false;
		}
		return method.getReturnType().getPackage().equals(Faker.class.getPackage());
	}

	private void describe(Method method) {
		System.out.print("* *" + method.getName() + "*:");
		Arrays.asList(method.getReturnType().getDeclaredMethods()).stream().filter(m -> m.getParameters().length == 0)
				.map(m -> m.getName()).sorted().forEach(n -> System.out.print(" " + n));
		System.out.println("");
	}

	public void showFakerHelp() {
		Arrays.asList(Faker.class.getDeclaredMethods()).stream().filter(this::accept)
				.sorted((m1, m2) -> m1.getName().compareTo(m2.getName())).forEach(m -> describe(m));
	}

	public boolean isSet() {
		return !fakerFields.isEmpty() || !simpleFields.isEmpty();
	}

}
