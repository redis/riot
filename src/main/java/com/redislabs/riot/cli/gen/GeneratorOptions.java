package com.redislabs.riot.cli.gen;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.github.javafaker.Faker;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.TagField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.generator.GeneratorReader;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
public class GeneratorOptions {

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@Option(names = "--faker", arity = "1..*", description = "SpEL expression to generate a field", paramLabel = "<name=SpEL>")
	private Map<String, String> fakerFields = new LinkedHashMap<>();
	@Option(names = { "-d",
			"--data" }, arity = "0..*", description = "Field sizes in bytes", paramLabel = "<field=size>")
	private Map<String, Integer> simpleFields = new LinkedHashMap<>();
	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = { "--faker-help" }, description = "Show all available Faker properties")
	private boolean fakerHelp;
	@Option(names = {
			"--faker-index" }, description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
	private String fakerIndex;

	private String expression(Field field) {
		if (field instanceof TextField) {
			return "lorem.paragraph";
		}
		if (field instanceof TagField) {
			return "number.digits(10)";
		}
		if (field instanceof GeoField) {
			return "address.longitude.concat(',').concat(address.latitude)";
		}
		return "number.randomDouble(3,-1000,1000)";
	}

	private String quotes(String field, String expression) {
		return "\"" + field + "=" + expression + "\"";
	}

	public GeneratorReader reader(RedisOptions redisOptions) {
		GeneratorReader reader = new GeneratorReader().locale(locale).fakerFields(fakerFields)
				.simpleFields(simpleFields);
		if (fakerIndex != null) {
			RediSearchCommands<String, String> ft = redisOptions.rediSearchClient().connect().sync();
			IndexInfo info = RediSearchUtils.getInfo(ft.indexInfo(fakerIndex));
			Map<String, String> fakerFields = new LinkedHashMap<>();
			info.fields().forEach(f -> {
				String fieldName = f.name();
				String expression = expression(f);
				fakerFields.put(fieldName, expression);
			});
			reader.fakerFields(fakerFields);
			log.info("Adding introspected fields: {}", String.join(" ", fakerArgs(fakerFields)));
		}
		return reader;
	}

	private List<String> fakerArgs(Map<String, String> fakerFields) {
		List<String> args = new ArrayList<>();
		fakerFields.forEach((k, v) -> args.add(quotes(k, v)));
		return args;
	}

	public void run() {
		if (fakerHelp) {
			Arrays.asList(Faker.class.getDeclaredMethods()).stream().filter(this::accept)
					.sorted((m1, m2) -> m1.getName().compareTo(m2.getName())).forEach(m -> describe(System.out, m));
			return;
		}
	}

	private boolean accept(Method method) {
		if (EXCLUDES.contains(method.getName())) {
			return false;
		}
		return method.getReturnType().getPackage().equals(Faker.class.getPackage());
	}

	private void describe(PrintStream stream, Method method) {
		stream.print("* *" + method.getName() + "*:");
		Arrays.asList(method.getReturnType().getDeclaredMethods()).stream().filter(m -> m.getParameters().length == 0)
				.map(m -> m.getName()).sorted().forEach(n -> stream.print(" " + n));
		stream.println("");
	}

}
