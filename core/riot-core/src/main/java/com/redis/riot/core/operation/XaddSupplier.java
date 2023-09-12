package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.XAddArgs;

public class XaddSupplier extends AbstractFilterMapOperationBuilder<XaddSupplier> {

    private long maxlen;

    private boolean approximateTrimming;

    public XaddSupplier maxlen(long maxlen) {
        this.maxlen = maxlen;
        return this;
    }

    public XaddSupplier approximateTrimming(boolean approximateTrimming) {
        this.approximateTrimming = approximateTrimming;
        return this;
    }

    @Override
    public Xadd<String, String, Map<String, Object>> operation() {
        Xadd<String, String, Map<String, Object>> operation = new Xadd<>();
        operation.setBody(map());
        operation.setArgs(args());
        return operation;
    }

    private XAddArgs args() {
        XAddArgs args = new XAddArgs();
        if (maxlen > 0) {
            args.maxlen(maxlen);
        }
        args.approximateTrimming(approximateTrimming);
        return args;
    }

}
