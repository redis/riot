package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.RedisOperation;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractKeyCommand {

    @SuppressWarnings("unused")
    @Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeoutField;
    @Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long timeoutDefault = 60;

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return configureKeyCommandBuilder(RedisOperation.expire()).timeout(numberFieldExtractor(Long.class, timeoutField, timeoutDefault)).build();
    }

}
