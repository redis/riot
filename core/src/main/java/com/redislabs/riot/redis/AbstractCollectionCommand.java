package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.CommandBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.Map;

@CommandLine.Command
public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

    @Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
    private String memberSpace = "";
    @Option(names = {"-m", "--members"}, arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
    private String[] memberFields = new String[0];

    protected <B extends CommandBuilder.CollectionCommandBuilder<?, Map<String, Object>, B>> B configureCollectionCommandBuilder(B builder) {
        return configureKeyCommandBuilder(builder).memberIdConverter(idMaker(memberSpace, memberFields));
    }

}
