package com.redis.riot.core;

import java.io.PrintWriter;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Mode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.util.KeyComparison;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class Replication extends AbstractExport {

    public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";

    public static final String STEP_LIVE = "live";

    public static final String STEP_SCAN = "scan";

    public static final String STEP_COMPARE = "compare";

    public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

    private final Logger log = LoggerFactory.getLogger(Replication.class);

    private final AbstractRedisClient targetClient;

    private PrintWriter out = new PrintWriter(System.out);

    private ReplicationMode mode = ReplicationMode.SNAPSHOT;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

    private boolean noVerify;

    private boolean showDiff;

    private Duration ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE;

    private KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();

    private RedisReaderOptions targetReaderOptions = new RedisReaderOptions();

    private RedisWriterOptions targetWriterOptions = new RedisWriterOptions();

    public void setTargetReaderOptions(RedisReaderOptions targetReaderOptions) {
        this.targetReaderOptions = targetReaderOptions;
    }

    public void setTargetWriterOptions(RedisWriterOptions targetWriterOptions) {
        this.targetWriterOptions = targetWriterOptions;
    }

    public void setProcessorOptions(KeyValueProcessorOptions options) {
        this.processorOptions = options;
    }

    public void setMode(ReplicationMode mode) {
        this.mode = mode;
    }

    public void setNoVerify(boolean noVerify) {
        this.noVerify = noVerify;
    }

    public void setTtlTolerance(Duration tolerance) {
        this.ttlTolerance = tolerance;
    }

    public void setValueType(ValueType type) {
        this.valueType = type;
    }

    public void setShowDiff(boolean showDiff) {
        this.showDiff = showDiff;
    }

    public Replication(AbstractRedisClient client, AbstractRedisClient targetClient) {
        super(client);
        this.targetClient = targetClient;
    }

    public void setOut(PrintWriter out) {
        this.out = out;
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
                SimpleFlow scanFlow = flow("scan").start(build(scanStep())).build();
                SimpleFlow liveFlow = flow("live").start(build(liveStep())).build();
                SimpleFlow replicateFlow = flow("replicate").split(asyncTaskExecutor()).add(liveFlow, scanFlow).build();
                JobFlowBuilder live = jobBuilder().start(replicateFlow);
                if (shouldCompare()) {
                    live.next(compareStep());
                }
                return live.build().build();
            case LIVEONLY:
                return jobBuilder().start(build(liveStep())).build();
            case SNAPSHOT:
                SimpleJobBuilder snapshot = jobBuilder().start(build(scanStep()));
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
        return !noVerify && !getStepOptions().isDryRun() && processorOptions.isEmpty();
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep() {
        return step(STEP_SCAN, reader(ByteArrayCodec.INSTANCE));
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step(String name, RedisItemReader<byte[], byte[]> reader) {
        reader.setName(name + "-reader");
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = step(name).reader(reader).writer(writer());
        step.processor(processor());
        if (log.isDebugEnabled()) {
            step.addWriteListener(new KeyValueWriteListener(log));
        }
        return step;
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep() {
        checkKeyspaceNotificationsConfig();
        RedisItemReader<byte[], byte[]> reader = reader(ByteArrayCodec.INSTANCE);
        reader.setMode(Mode.LIVE);
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = step(STEP_LIVE, reader);
        step.flushingInterval(getReaderOptions().getFlushingInterval());
        step.idleTimeout(getReaderOptions().getIdleTimeout());
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
        StepBuilder<KeyComparison, KeyComparison> step = step(STEP_COMPARE).reader(reader).writer(writer);
        if (showDiff) {
            step.addWriteListener(new KeyComparisonDiffLogger(out));
        }
        step.addExecutionListener(new KeyComparisonSummaryLogger(writer, out));
        return build(step);
    }

    private KeyComparisonItemReader comparisonReader() {
        RedisItemReader<String, String> leftReader = reader(StringCodec.UTF8);
        RedisItemReader<String, String> rightReader = targetReader();
        KeyComparisonItemReader reader = new KeyComparisonItemReader(leftReader, rightReader);
        reader.setTtlTolerance(ttlTolerance);
        reader.setName("compare-reader");
        return reader;
    }

    private RedisItemReader<String, String> targetReader() {
        return reader(targetClient, StringCodec.UTF8, targetReaderOptions);
    }

    private KeyComparisonStatusCountItemWriter comparisonWriter() {
        return new KeyComparisonStatusCountItemWriter();
    }

    private ItemWriter<KeyValue<byte[]>> writer() {
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetClient, ByteArrayCodec.INSTANCE);
        targetWriterOptions.configure(writer);
        writer.setValueType(valueType);
        return writer;
    }

    private ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> processor() {
        if (processorOptions.isEmpty()) {
            return null;
        }
        return new FunctionItemProcessor<>(processorOptions.operator(ByteArrayCodec.INSTANCE));
    }

}
