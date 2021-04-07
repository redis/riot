package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.RedisOperationBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.Map;

@CommandLine.Command
public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

    @Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
    private String memberSpace = "";
    @Option(arity = "1..*", names = {"-m", "--members"}, description = "Member field names for collections", paramLabel = "<fields>")
    private String[] memberFields;

    protected <B extends RedisOperationBuilder.AbstractCollectionOperationBuilder<String, String, Map<String, Object>, B>> B configureCollectionCommandBuilder(B builder) {
        return configureKeyCommandBuilder(builder).memberIdConverter(idMaker(memberSpace, memberFields));
    }

}
