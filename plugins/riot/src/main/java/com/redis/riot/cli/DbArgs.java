package com.redis.riot.cli;

import com.redis.riot.db.DataSourceOptions;

import picocli.CommandLine.Option;

public class DbArgs {

    @Option(names = "--driver", description = "Fully qualified name of the JDBC driver.", paramLabel = "<class>")
    String driver;

    @Option(names = "--url", required = true, description = "JDBC URL to connect to the database.", paramLabel = "<string>")
    String url;

    @Option(names = "--username", description = "Login username of the database.", paramLabel = "<string>")
    String username;

    @Option(names = "--password", arity = "0..1", interactive = true, description = "Login password of the database.", paramLabel = "<pwd>")
    String password;

    public DataSourceOptions dataSourceOptions() {
        DataSourceOptions options = new DataSourceOptions();
        options.setDriver(driver);
        options.setUrl(url);
        options.setUsername(username);
        options.setPassword(password);
        return options;
    }

}
