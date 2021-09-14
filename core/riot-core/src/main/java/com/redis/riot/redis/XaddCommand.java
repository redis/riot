package com.redis.riot.redis;

import io.lettuce.core.XAddArgs;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.operation.Xadd;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends AbstractKeyCommand {

    @CommandLine.Mixin
    private FilteringOptions filteringOptions = new FilteringOptions();
    @SuppressWarnings("unused")
    @Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @SuppressWarnings("unused")
    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
    private boolean approximateTrimming;

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        XAddArgs args = new XAddArgs();
        if (maxlen != null) {
            args.maxlen(maxlen);
        }
        args.approximateTrimming(approximateTrimming);
        return Xadd.key(key()).body(filteringOptions.converter()).args(args).build();
    }

}
