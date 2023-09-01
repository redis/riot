package com.redis.riot.cli;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import picocli.CommandLine.ArgGroup;

abstract class AbstractLoggingCommand<C extends IO> extends AbstractCommand<C> {

    private static final String RIOT_LOGGER = "com.redis";

    private static final String AWS_LOGGER = "com.amazonaws";

    private static final String LETTUCE_LOGGER = "io.lettuce";

    private static final String NETTY_LOGGER = "io.netty";

    private static final String JLINE_LOGGER = "org.jline";

    protected Logger logger;

    @ArgGroup(exclusive = false, heading = "Logging options%n")
    private LoggingArgs loggingArgs = new LoggingArgs();

    @Override
    protected void setup() {
        setLogLevel(AWS_LOGGER, Level.ERROR);
        setLogLevel(LETTUCE_LOGGER, Level.WARN);
        setLogLevel(NETTY_LOGGER, Level.WARN);
        setLogLevel(JLINE_LOGGER, Level.WARN);
        setLogLevel(RIOT_LOGGER, loggingArgs.getLevel());
    }

}
