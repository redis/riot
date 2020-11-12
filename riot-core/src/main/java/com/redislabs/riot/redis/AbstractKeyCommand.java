package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.support.AbstractKeyCommandItemWriterBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractKeyCommand extends AbstractRedisCommand<Map<String, Object>> {

    @Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
    private String keyspace;
    @Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
    private String[] keys = new String[0];

    protected <B extends AbstractKeyCommandItemWriterBuilder<Map<String, Object>, B>> B configure(B builder)
	    throws Exception {
	return super.configure(builder).keyConverter(idMaker(keyspace, keys));
    }

}
