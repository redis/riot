package com.redislabs.riot.cli;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import com.github.javafaker.Faker;
import com.redislabs.riot.batch.generator.GeneratorReader;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "gen", description = "Import generated data")
public class GeneratorCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Generator options%n", order = 2)
	private GeneratorOptions options = new GeneratorOptions();

	@Override
	protected GeneratorReader reader(RedisConnectionOptions redisOptions) {
		return options.reader();
	}

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@Override
	public void run() {
		if (options.isFakerHelp()) {
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
		System.out.print("* *" + method.getName() + "*:");
		Arrays.asList(method.getReturnType().getDeclaredMethods()).stream().filter(m -> m.getParameters().length == 0)
				.map(m -> m.getName()).sorted().forEach(n -> System.out.print(" " + n));
		System.out.println("");
	}

}
