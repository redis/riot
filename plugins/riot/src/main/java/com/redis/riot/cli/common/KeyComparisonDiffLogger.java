package com.redis.riot.cli.common;

import java.io.PrintWriter;
import java.text.MessageFormat;
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
        items.stream().filter(c -> c.getStatus() != Status.OK).forEach(this::log);
    }

    @Override
    public void onWriteError(Exception exception, List<? extends KeyComparison> items) {
        // do nothing
    }

    public void log(KeyComparison comparison) {
        switch (comparison.getStatus()) {
            case MISSING:
                log("Missing key \"{0}\"", comparison.getSource().getKey());
                break;
            case TTL:
                log("TTL mismatch on key \"{0}\": {1} != {2}", comparison.getSource().getKey(), comparison.getSource().getTtl(),
                        comparison.getTarget().getTtl());
                break;
            case TYPE:
                log("Type mismatch on key \"{0}\": {1} != {2}", comparison.getSource().getKey(),
                        comparison.getSource().getType(), comparison.getTarget().getType());
                break;
            case VALUE:
                log("Value mismatch on {0} \"{1}\"", comparison.getSource().getType(), comparison.getSource().getKey());
                break;
            default:
                break;
        }
    }

    private void log(String pattern, Object... arguments) {
        out.println(MessageFormat.format(pattern, arguments));
    }

}
