package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisExpireItemWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire")
public class ExpireCommand extends AbstractKeyCommand {

    @Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeoutField;
    @Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long timeoutDefault = 60;

    @Override
    public RedisExpireItemWriter<Map<String, Object>> writer() throws Exception {
	return configure(RedisExpireItemWriter.<Map<String, Object>>builder()
		.timeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault))).build();
    }

}
