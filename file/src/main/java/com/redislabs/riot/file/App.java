package com.redislabs.riot.file;

import com.redislabs.riot.RiotApp;
import org.springframework.batch.item.file.transform.Range;
import picocli.CommandLine;

@CommandLine.Command(name = "riot-file", subcommands = {FileImportCommand.class, FileExportCommand.class})
public class App extends RiotApp {

    @Override
    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(Range.class, new RangeConverter());
        super.registerConverters(commandLine);
    }

    public static void main(String[] args) {
        System.exit(new App().execute(args));
    }
}
