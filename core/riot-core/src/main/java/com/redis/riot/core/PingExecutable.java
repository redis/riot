package com.redis.riot.core;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.metrics.CommandMetrics.CommandLatency;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;

public class PingExecutable implements Executable {

    private final AbstractRedisClient client;

    public static final int DEFAULT_ITERATIONS = 1;

    public static final int DEFAULT_COUNT = 10;

    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    public static final double[] DEFAULT_PERCENTILES = DefaultCommandLatencyCollectorOptions.DEFAULT_TARGET_PERCENTILES;

    private final PrintStream printStream;

    private int iterations = DEFAULT_ITERATIONS;

    private int count = DEFAULT_COUNT;

    private TimeUnit timeUnit = DEFAULT_TIME_UNIT;

    private boolean latencyDistribution;

    private double[] percentiles = DEFAULT_PERCENTILES;

    private Duration sleep;

    public PingExecutable(AbstractRedisClient client, PrintStream printStream) {
        this.client = client;
        this.printStream = printStream;
    }

    public void setSleep(Duration sleep) {
        this.sleep = sleep;
    }

    public Duration getSleep() {
        return sleep;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit unit) {
        this.timeUnit = unit;
    }

    public boolean isLatencyDistribution() {
        return latencyDistribution;
    }

    public void setLatencyDistribution(boolean latencyDistribution) {
        this.latencyDistribution = latencyDistribution;
    }

    public double[] getPercentiles() {
        return percentiles;
    }

    public void setPercentiles(double[] percentiles) {
        this.percentiles = percentiles;
    }

    @Override
    public void execute() {
        try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
            for (int iteration = 0; iteration < iterations; iteration++) {
                execute(connection);
                if (sleep != null) {
                    try {
                        Thread.sleep(sleep.toMillis());
                    } catch (InterruptedException e) {
                        // Restore interrupted state...
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void execute(StatefulRedisModulesConnection<String, String> connection) {
        LatencyStats stats = new LatencyStats();
        for (int index = 0; index < count; index++) {
            long startTime = System.nanoTime();
            String reply = connection.sync().ping();
            Assert.isTrue("pong".equalsIgnoreCase(reply), "Invalid PING reply received: " + reply);
            stats.recordLatency(System.nanoTime() - startTime);
        }
        Histogram histogram = stats.getIntervalHistogram();
        if (latencyDistribution) {
            histogram.outputPercentileDistribution(printStream, (double) timeUnit.toNanos(1));
        }
        Map<Double, Long> percentileMap = new TreeMap<>();
        for (double targetPercentile : percentiles) {
            long percentile = toTimeUnit(histogram.getValueAtPercentile(targetPercentile));
            percentileMap.put(targetPercentile, percentile);
        }
        long min = toTimeUnit(histogram.getMinValue());
        long max = toTimeUnit(histogram.getMaxValue());
        CommandLatency latency = new CommandLatency(min, max, percentileMap);
        printStream.println(latency.toString());
    }

    private long toTimeUnit(long value) {
        return timeUnit.convert(value, TimeUnit.NANOSECONDS);
    }

}
