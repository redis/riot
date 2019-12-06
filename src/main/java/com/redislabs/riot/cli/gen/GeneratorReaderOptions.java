package com.redislabs.riot.cli.gen;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redislabs.riot.batch.generator.GeneratorReader;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class GeneratorReaderOptions {

	@Option(arity = "1..*", names = "--faker", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fakerFields = new LinkedHashMap<>();
	@Option(names = { "-d",
			"--data" }, arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> simpleFields = new LinkedHashMap<>();
	@Option(names = { "-l",
			"--locale" }, description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;

	public GeneratorReader reader() {
		SpelExpressionParser parser = new SpelExpressionParser();
		Map<String, Expression> expressionMap = new LinkedHashMap<String, Expression>();
		fakerFields.forEach((k, v) -> expressionMap.put(k, parser.parseExpression(v)));
		return new GeneratorReader().locale(locale).fieldExpressions(expressionMap).fieldSizes(simpleFields);
	}

	public boolean isSet() {
		return !fakerFields.isEmpty() || !simpleFields.isEmpty();
	}

}
