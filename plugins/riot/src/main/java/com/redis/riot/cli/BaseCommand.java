package com.redis.riot.cli;

import java.util.ResourceBundle;

import org.slf4j.helpers.MessageFormatter;

import com.redis.riot.cli.common.ManifestVersionProvider;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * @author Julien Ruaux
 */
@Command(versionProvider = ManifestVersionProvider.class, resourceBundle = "com.redis.riot.Messages", usageHelpAutoWidth = true)
public class BaseCommand {

    static {
        if (System.getenv().containsKey("RIOT_NO_COLOR")) {
            System.setProperty("picocli.ansi", "false");
        }
    }

    @Spec
    CommandSpec spec;

    ResourceBundle bundle = ResourceBundle.getBundle("com.redis.riot.Messages");

    @Option(names = { "-H", "--help" }, usageHelp = true, description = "Show this help message and exit.")
    boolean helpRequested;
    
    @ArgGroup(exclusive=false)
    private LoggingArgs loggingArgs = new LoggingArgs();

    protected String $(String key, Object... args) {
        if (null == args || args.length == 0) {
            return bundle.getString(key);
        }
        return MessageFormatter.arrayFormat(bundle.getString(key), args).getMessage();
    }

}
