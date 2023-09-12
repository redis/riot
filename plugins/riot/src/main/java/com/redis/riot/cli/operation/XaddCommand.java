package com.redis.riot.cli.operation;

import com.redis.riot.core.operation.XaddSupplier;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends OperationCommand {

    @Mixin
    private FieldFilteringArgs filteringOptions = new FieldFilteringArgs();

    @Option(names = "--maxlen", description = "Stream maxlen.", paramLabel = "<int>")
    private long maxlen;

    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag).")
    private boolean approximateTrimming;

    public FieldFilteringArgs getFilteringOptions() {
        return filteringOptions;
    }

    public void setFilteringOptions(FieldFilteringArgs filteringOptions) {
        this.filteringOptions = filteringOptions;
    }

    public long getMaxlen() {
        return maxlen;
    }

    public void setMaxlen(long maxlen) {
        this.maxlen = maxlen;
    }

    public boolean isApproximateTrimming() {
        return approximateTrimming;
    }

    public void setApproximateTrimming(boolean approximateTrimming) {
        this.approximateTrimming = approximateTrimming;
    }
    
    @Override
    protected XaddSupplier operationBuilder() {
        XaddSupplier supplier = new XaddSupplier();
        supplier.approximateTrimming(approximateTrimming);
        supplier.maxlen(maxlen);
        return supplier;
    }

}
