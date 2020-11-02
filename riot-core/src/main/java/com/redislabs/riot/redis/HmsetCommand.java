package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisHashItemWriter;

import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;

import picocli.CommandLine.Command;

@Command(name = "hmset")
public class HmsetCommand extends AbstractKeyCommand {

    @Override
    public RedisHashItemWriter<String, String, Map<String, Object>> writer() throws Exception {
        return configure(RedisHashItemWriter.<Map<String, Object>> builder()
                .mapConverter(new MapFlattener<>(new ObjectToStringConverter()))).build();
    }

}
