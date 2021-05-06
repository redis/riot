package com.redislabs.riot;

import lombok.Data;
import picocli.CommandLine;

@Data
@CommandLine.Command(usageHelpAutoWidth = true)
public class HelpCommand {

    @SuppressWarnings("unused")
    @CommandLine.Option(names = {"-H", "--help"}, usageHelp = true, description = "Show this help message and exit")
    private boolean helpRequested;

}
