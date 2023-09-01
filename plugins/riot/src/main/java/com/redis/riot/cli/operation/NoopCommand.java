package com.redis.riot.cli.operation;

import java.util.Map;

import com.redis.riot.core.operation.OperationBuilder;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.operation.Noop;

import picocli.CommandLine.Command;

@Command(name = "noop", description = "Do nothing")
public class NoopCommand implements OperationBuilder {

    @Override
    public Operation<String, String, Map<String, Object>> build() {
        return new Noop<>();
    }

}
