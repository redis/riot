package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import com.redislabs.riot.cli.redis.command.EvalshaCommand;
import com.redislabs.riot.cli.redis.command.ExpireCommand;
import com.redislabs.riot.cli.redis.command.FtAddCommand;
import com.redislabs.riot.cli.redis.command.FtSugaddCommand;
import com.redislabs.riot.cli.redis.command.GeoaddCommand;
import com.redislabs.riot.cli.redis.command.HmsetCommand;
import com.redislabs.riot.cli.redis.command.LpushCommand;
import com.redislabs.riot.cli.redis.command.NoopCommand;
import com.redislabs.riot.cli.redis.command.RpushCommand;
import com.redislabs.riot.cli.redis.command.SaddCommand;
import com.redislabs.riot.cli.redis.command.SetCommand;
import com.redislabs.riot.cli.redis.command.XaddCommand;
import com.redislabs.riot.cli.redis.command.ZaddCommand;

import lombok.Data;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalshaCommand.class, ExpireCommand.class, FtAddCommand.class, FtSugaddCommand.class,
		GeoaddCommand.class, HmsetCommand.class, LpushCommand.class, NoopCommand.class, RpushCommand.class,
		SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class })
public abstract @Data class MapImportCommand extends ImportCommand<Map<String, Object>, Map<String, Object>>
		implements Runnable {

	@ArgGroup(exclusive = false, heading = "Processor options%n", order = 40)
	private ProcessorOptions processorOptions = new ProcessorOptions();

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return processorOptions.processor(postProcessor());
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> postProcessor() {
		return null;
	}

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

}
