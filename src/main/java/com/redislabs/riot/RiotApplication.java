package com.redislabs.riot;

import java.util.List;
import java.util.Locale;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.redislabs.riot.cli.BaseCommand;
import com.redislabs.riot.cli.in.ImportCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;

@SpringBootApplication
@Command(name = "riot", subcommands = { ImportCommand.class })
public class RiotApplication extends BaseCommand implements CommandLineRunner {

	/**
	 * Just to avoid picocli complain in Eclipse console
	 */
	@Option(names = "--spring.output.ansi.enabled", hidden = true)
	private String ansiEnabled;

	public static void main(String[] args) {
		SpringApplication.run(RiotApplication.class, args);
	}

	@Override
	public void run(String... args) {
		CommandLine commandLine = new CommandLine(this);
		commandLine.registerConverter(Locale.class, s -> new Locale.Builder().setLanguageTag(s).build());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		RunLast handler = new RunLast();
		handler.useOut(System.out);
		DefaultExceptionHandler<List<Object>> exceptionHandler = CommandLine.defaultExceptionHandler();
		exceptionHandler.useErr(System.err);
		commandLine.parseWithHandlers(handler, exceptionHandler, args);
	}

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

}
