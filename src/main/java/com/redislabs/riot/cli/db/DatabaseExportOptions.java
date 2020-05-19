package com.redislabs.riot.cli.db;

import lombok.Getter;
import picocli.CommandLine;

public class DatabaseExportOptions extends DatabaseOptions {

    @Getter
    @CommandLine.Option(names = "--sql", description = "Insert SQL statement", paramLabel = "<sql>")
    private String sql;
    @Getter
    @CommandLine.Option(names = "--no-assert-updates", description = "Disable insert verification")
    private boolean noAssertUpdates;

}
