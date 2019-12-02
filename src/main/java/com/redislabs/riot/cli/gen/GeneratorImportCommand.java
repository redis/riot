package com.redislabs.riot.cli.gen;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import com.github.javafaker.Faker;
import com.redislabs.riot.batch.generator.GeneratorReader;
import com.redislabs.riot.cli.MapImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "gen", description = "Generate data")
public class GeneratorImportCommand extends MapImportCommand implements Runnable {

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@ArgGroup(exclusive = false, heading = "Generator options%n", order = 2)
	private GeneratorReaderOptions options = new GeneratorReaderOptions();
	@Option(names = { "--faker-help" }, description = "Show all available Faker properties")
	private boolean fakerHelp;

	@Override
	protected GeneratorReader reader() {
		return options.reader();
	}

	@Override
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
