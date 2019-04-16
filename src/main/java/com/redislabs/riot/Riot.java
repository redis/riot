package com.redislabs.riot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.redislabs.riot.cli.MainCommand;

import picocli.CommandLine;

@SpringBootApplication
public class Riot implements CommandLineRunner {

	@Autowired
	private MainCommand riotCommand;

	public static void main(String[] args) {
		SpringApplication.run(Riot.class, args);
	}

	@Override
	public void run(String... args) {
		new CommandLine(riotCommand).setCaseInsensitiveEnumValuesAllowed(true).parseWithHandlers(
				new CommandLine.RunLast().useOut(System.out), CommandLine.defaultExceptionHandler().useErr(System.err),
				args);
	}
}
