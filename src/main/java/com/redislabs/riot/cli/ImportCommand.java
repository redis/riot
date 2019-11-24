package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.redis.commands.EvalshaCommand;
import com.redislabs.riot.cli.redis.commands.ExpireCommand;
import com.redislabs.riot.cli.redis.commands.FtAddCommand;
import com.redislabs.riot.cli.redis.commands.FtSugaddCommand;
import com.redislabs.riot.cli.redis.commands.GeoaddCommand;
import com.redislabs.riot.cli.redis.commands.HmsetCommand;
import com.redislabs.riot.cli.redis.commands.LpushCommand;
import com.redislabs.riot.cli.redis.commands.NoopCommand;
import com.redislabs.riot.cli.redis.commands.RpushCommand;
import com.redislabs.riot.cli.redis.commands.SaddCommand;
import com.redislabs.riot.cli.redis.commands.SetCommand;
import com.redislabs.riot.cli.redis.commands.XaddCommand;
import com.redislabs.riot.cli.redis.commands.ZaddCommand;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalshaCommand.class, ExpireCommand.class, FtAddCommand.class, FtSugaddCommand.class,
		GeoaddCommand.class, HmsetCommand.class, LpushCommand.class, NoopCommand.class, RpushCommand.class,
		SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class })
@Slf4j
public abstract class ImportCommand extends TransferCommand {

	public void execute(ItemWriter<Map<String, Object>> writer) {
		ItemReader<Map<String, Object>> reader;
		try {
			reader = reader();
		} catch (Exception e) {
			log.error("Could not initialize reader", e);
			return;
		}
		execute(reader, writer);
	}

	protected abstract ItemReader<Map<String, Object>> reader() throws Exception;

}
