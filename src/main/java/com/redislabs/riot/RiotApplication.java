package com.redislabs.riot;

import java.util.List;
import java.util.Locale;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.redislabs.riot.cli.RootCommand;

import picocli.CommandLine;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.RunLast;

@SpringBootApplication
public class RiotApplication implements CommandLineRunner {

	private RootCommand riot = new RootCommand();

	public static void main(String[] args) {
		SpringApplication.run(RiotApplication.class, args);
	}

	@Override
	public void run(String... args) {
		CommandLine commandLine = new CommandLine(riot);
		commandLine.registerConverter(Locale.class, s -> new Locale.Builder().setLanguageTag(s).build());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		RunLast handler = new RunLast();
		handler.useOut(System.out);
		DefaultExceptionHandler<List<Object>> exceptionHandler = CommandLine.defaultExceptionHandler();
		exceptionHandler.useErr(System.err);
		commandLine.parseWithHandlers(handler, exceptionHandler, args);
	}

}
