package com.redislabs.riot;

import lombok.Getter;
import lombok.Setter;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(usageHelpAutoWidth = true)
public class HelpCommand {

    @SuppressWarnings("unused")
    @Setter
    @Getter
    @CommandLine.Option(names = {"-H", "--help"}, usageHelp = true, description = "Show this help message and exit")
    private boolean helpRequested;

}
