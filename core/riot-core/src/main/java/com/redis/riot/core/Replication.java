package com.redis.riot.core;

import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Mode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.util.KeyComparison;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class Replication extends AbstractExport<byte[], byte[]> {

    public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";

    public static final String STEP_LIVE = "live";

    public static final String STEP_SCAN = "scan";

    public static final String STEP_COMPARE = "compare";

    public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

    private final Logger log = LoggerFactory.getLogger(Replication.class);

    private final AbstractRedisClient targetClient;

    private final PrintWriter out;

    private ReplicationMode mode = ReplicationMode.SNAPSHOT;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

    private KeyComparisonOptions comparisonOptions = new KeyComparisonOptions();

    private ReadFrom targetReadFrom;

    private RedisWriterOptions targetWriterOptions = new RedisWriterOptions();

    public Replication(AbstractRedisClient client, AbstractRedisClient targetClient, PrintWriter out) {
        super(client, ByteArrayCodec.INSTANCE);
        this.targetClient = targetClient;
        this.out = out;
    }

    public void setTargetReadFrom(ReadFrom readFrom) {
        this.targetReadFrom = readFrom;
    }

    public void setTargetWriterOptions(RedisWriterOptions options) {
        this.targetWriterOptions = options;
    }

    public void setComparisonOptions(KeyComparisonOptions options) {
        this.comparisonOptions = options;
    }

    public void setMode(ReplicationMode mode) {
        this.mode = mode;
    }

    public void setValueType(ValueType type) {
        this.valueType = type;
    }

    @Override
    protected ValueType getValueType() {
        return valueType;
    }

    @Override
    protected Job job() {
        switch (mode) {
            case COMPARE:
                return jobBuilder().start(compareStep()).build();
            case LIVE:
                SimpleFlow scanFlow = flow("scan").start(scanStep().build()).build();
                SimpleFlow liveFlow = flow("live").start(liveStep().build()).build();
                SimpleFlow replicateFlow = flow("replicate").split(asyncTaskExecutor()).add(liveFlow, scanFlow).build();
                JobFlowBuilder live = jobBuilder().start(replicateFlow);
                if (shouldCompare()) {
                    live.next(compareStep());
                }
                return live.build().build();
            case LIVEONLY:
                return jobBuilder().start(liveStep().build()).build();
            case SNAPSHOT:
                SimpleJobBuilder snapshot = jobBuilder().start(scanStep().build());
                if (shouldCompare()) {
                    snapshot.next(compareStep());
                }
                return snapshot.build();
            default:
                throw new IllegalArgumentException("Unknown replication mode: " + mode);
        }
    }

    private TaskExecutor asyncTaskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    private static FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name);
    }

    private boolean shouldCompare() {
        return !comparisonOptions.isNoVerify() && !getStepOptions().isDryRun() && processorOptions.isEmpty();
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep() {
        return step(STEP_SCAN, reader(codec));
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step(String name, RedisItemReader<byte[], byte[]> reader) {
        reader.setName(name + "-reader");
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = createStep();
        step.name(name);
        step.reader(reader);
        step.writer(writer());
        step.processor(keyValueProcessor());
        if (log.isDebugEnabled()) {
            step.addWriteListener(new KeyValueWriteListener(log));
        }
        return step;
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep() {
        checkKeyspaceNotificationsConfig();
        RedisItemReader<byte[], byte[]> reader = reader(codec);
        reader.setMode(Mode.LIVE);
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = step(STEP_LIVE, reader);
        step.flushingInterval(readerOptions.getFlushingInterval());
        step.idleTimeout(readerOptions.getIdleTimeout());
        return step;
    }

    private void checkKeyspaceNotificationsConfig() {
        StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
        try {
            String config = connection.sync().configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
                    .getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
            if (!config.contains("K")) {
                log.error("Keyspace notifications not property configured ({}={}). Make sure it contains at least \"K\".",
                        CONFIG_NOTIFY_KEYSPACE_EVENTS, config);
            }
        } catch (RedisException e) {
            // CONFIG command might not be available. Ignore.
        }
    }

    private Step compareStep() {
        KeyComparisonItemReader reader = comparisonReader();
        KeyComparisonStatusCountItemWriter writer = comparisonWriter();
        StepBuilder<KeyComparison, KeyComparison> step = createStep();
        step.name(STEP_COMPARE);
        step.reader(reader);
        step.writer(writer);
        if (comparisonOptions.isShowDiff()) {
            step.addWriteListener(new KeyComparisonDiffLogger(out));
        }
        step.addExecutionListener(new KeyComparisonSummaryLogger(writer, out));
        return step.build();
    }

    private KeyComparisonItemReader comparisonReader() {
        RedisItemReader<String, String> leftReader = reader(StringCodec.UTF8);
        KeyValueItemProcessor<String, String> rightReader = new KeyValueItemProcessor<>(targetClient, StringCodec.UTF8);
        rightReader.setPoolSize(targetWriterOptions.getPoolSize());
        rightReader.setReadFrom(targetReadFrom);
        rightReader.setMemoryUsageLimit(readerOptions.getMemoryUsageLimit());
        rightReader.setMemoryUsageSamples(readerOptions.getMemoryUsageSamples());
        KeyComparisonItemReader reader = new KeyComparisonItemReader(leftReader, rightReader);
        reader.setTtlTolerance(comparisonOptions.getTtlTolerance());
        reader.setName("compare-reader");
        return reader;
    }

    private KeyComparisonStatusCountItemWriter comparisonWriter() {
        return new KeyComparisonStatusCountItemWriter();
    }

    private ItemWriter<KeyValue<byte[]>> writer() {
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetClient, codec);
        targetWriterOptions.configure(writer);
        writer.setValueType(valueType);
        return writer;
    }

}
