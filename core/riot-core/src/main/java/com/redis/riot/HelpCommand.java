package com.redis.riot;

import lombok.Data;
import picocli.CommandLine;

@Data
@CommandLine.Command(usageHelpAutoWidth = true)
public class HelpCommand {

	@CommandLine.Option(names = { "-H", "--help" }, usageHelp = true, description = "Show this help message and exit")
	private boolean helpRequested;

}
