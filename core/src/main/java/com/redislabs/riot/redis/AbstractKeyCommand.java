package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.Map;

@CommandLine.Command
public abstract class AbstractKeyCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Option(names = {"-p", "--keyspace"}, description = "Keyspace prefix", paramLabel = "<str>")
    private String keyspace = "";
    @Option(names = {"-k", "--keys"}, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
    private String[] keys = new String[0];

    protected <B extends CommandBuilder.KeyCommandBuilder<?, Map<String, Object>, B>> B configure(B builder) {
        builder.keyConverter(idMaker(keyspace, keys));
        return builder;
    }

}
