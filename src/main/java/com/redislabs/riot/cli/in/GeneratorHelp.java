package com.redislabs.riot.cli.in;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.core.convert.Property;

import com.github.javafaker.Faker;

import picocli.CommandLine.Command;

@Command(name = "faker-help", description = "Print all faker entities")
public class GeneratorHelp implements Runnable {

	private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

	@Override
	public void run() {
		Arrays.asList(Faker.class.getDeclaredMethods()).stream().filter(this::accept)
				.sorted((m1, m2) -> m1.getName().compareTo(m2.getName())).forEach(this::print);
	}

	public boolean accept(Method method) {
		if (EXCLUDES.contains(method.getName())) {
			return false;
		}
		return method.getReturnType().getPackage().equals(Faker.class.getPackage());
	}

	public void print(Method method) {
		System.out.println(method.getName() + ": "
				+ String.join(" ",
						Arrays.asList(method.getReturnType().getDeclaredMethods()).stream()
								.filter(m -> m.getParameters().length == 0).map(m -> describe(m))
								.collect(Collectors.toList())));
	}

	private String describe(Method method) {
		String description = method.getName();
		if (method.getParameters().length > 0) {
			description += "(";
			description += String.join(", ",
					Arrays.asList(method.getParameters()).stream().map(p -> describe(p)).collect(Collectors.toList()));
			description += ")";
		}
		return description;
	}

	private String describe(Parameter parameter) {
		return parameter.getType().getSimpleName();
	}

}
