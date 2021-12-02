package com.redis.riot.file;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.redis.riot.RiotApp;
import picocli.CommandLine.Command;

@Command(name = "riot-file", subcommands = {FileImportCommand.class, DumpFileImportCommand.class, FileExportCommand.class})
public class RiotFile extends RiotApp {

    public static void main(String[] args) {
        System.exit(new RiotFile().execute(args));
    }
    
    @Override
    protected void configureLogging() {
    	super.configureLogging();
    	Logger.getLogger("com.amazonaws").setLevel(Level.SEVERE);
    }

}
