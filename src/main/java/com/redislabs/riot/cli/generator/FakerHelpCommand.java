package com.redislabs.riot.cli.generator;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import com.github.javafaker.Faker;

import picocli.CommandLine.Command;

@Command(name = "faker-help", description = "Faker help")
public class FakerHelpCommand implements Runnable {

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@Override
	public void run() {
		Arrays.asList(Faker.class.getDeclaredMethods()).stream().filter(this::accept)
				.sorted((m1, m2) -> m1.getName().compareTo(m2.getName())).forEach(m -> describe(m));
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
