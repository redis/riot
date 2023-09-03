package com.redis.riot.cli;

import java.io.PrintWriter;
import java.util.List;

import org.springframework.batch.core.ItemWriteListener;

import com.redis.spring.batch.util.KeyComparison;
import com.redis.spring.batch.util.KeyComparison.Status;

public class KeyComparisonDiffLogger implements ItemWriteListener<KeyComparison> {

    private final PrintWriter out;

    public KeyComparisonDiffLogger(PrintWriter out) {
        this.out = out;
    }

    @Override
    public void beforeWrite(List<? extends KeyComparison> items) {
        // do nothing
    }

    @Override
    public void afterWrite(List<? extends KeyComparison> items) {
        items.stream().filter(c -> c.getStatus() != Status.OK).forEach(this::print);
    }

    @Override
    public void onWriteError(Exception exception, List<? extends KeyComparison> items) {
        // do nothing
    }

    public void print(KeyComparison comparison) {
        switch (comparison.getStatus()) {
            case MISSING:
                out.format("Missing key '%s'", comparison.getSource().getKey());
                break;
            case TTL:
                out.format("TTL mismatch on key '%s': %,d != %,d", comparison.getSource().getKey(),
                        comparison.getSource().getTtl(), comparison.getTarget().getTtl());
                break;
            case TYPE:
                out.format("Type mismatch on key '%s': %s != %s", comparison.getSource().getKey(),
                        comparison.getSource().getType(), comparison.getTarget().getType());
                break;
            case VALUE:
                out.format("Value mismatch on %s '%s'", comparison.getSource().getType(), comparison.getSource().getKey());
                break;
            default:
                break;
        }
    }

}
