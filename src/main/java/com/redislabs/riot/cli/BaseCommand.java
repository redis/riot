package com.redislabs.riot.cli;

import java.util.concurrent.Callable;

import org.springframework.batch.core.ExitStatus;

import picocli.CommandLine;

public class BaseCommand implements Callable<ExitStatus> {

	@Override
	public ExitStatus call() {
		new CommandLine(this).usage(System.out);
		return ExitStatus.COMPLETED;
	}

}
