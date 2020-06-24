package com.redislabs.riot.gen;

import com.redislabs.riot.RiotApp;
import picocli.CommandLine;

@CommandLine.Command(name = "riot-gen", subcommands = {GenerateCommand.class, FakerHelpCommand.class})
public class App extends RiotApp {

    public static void main(String[] args) {
        System.exit(new App().execute(args));
    }

}
