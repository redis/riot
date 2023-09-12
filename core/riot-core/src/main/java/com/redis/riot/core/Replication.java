package com.redis.riot.core;

import java.io.PrintWriter;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

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

    private static final String VARIABLE_SOURCE = "source";

    private static final String VARIABLE_TARGET = "target";

    private final PrintWriter out;

    private RedisOptions targetRedisClientOptions = new RedisOptions();

    private ReadFrom targetReadFrom;

    private RedisWriterOptions targetWriterOptions = new RedisWriterOptions();

    private ReplicationMode mode = ReplicationMode.SNAPSHOT;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

    private KeyComparisonOptions comparisonOptions = new KeyComparisonOptions();

    public Replication(PrintWriter out) {
        super(ByteArrayCodec.INSTANCE);
        this.out = out;
    }

    public void setTargetRedisClientOptions(RedisOptions targetRedisClientOptions) {
        this.targetRedisClientOptions = targetRedisClientOptions;
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
    protected RiotExecutionContext executionContext(RedisOptions redisClientOptions) {
        return new ReplicationExecutionContext(redisClientOptions, targetRedisClientOptions);
    }

    @Override
    protected Job job(RiotExecutionContext context) {
        Assert.isInstanceOf(ReplicationExecutionContext.class, context, "Execution context is not a replication context");
        ReplicationExecutionContext replicationContext = (ReplicationExecutionContext) context;
        switch (mode) {
            case COMPARE:
                return jobBuilder().start(compareStep(replicationContext)).build();
            case LIVE:
                SimpleFlow scanFlow = flow("scan").start(scanStep(replicationContext).build()).build();
                SimpleFlow liveFlow = flow("live").start(liveStep(replicationContext).build()).build();
                SimpleFlow replicateFlow = flow("replicate").split(asyncTaskExecutor()).add(liveFlow, scanFlow).build();
                JobFlowBuilder live = jobBuilder().start(replicateFlow);
                if (shouldCompare()) {
                    live.next(compareStep(replicationContext));
                }
                return live.build().build();
            case LIVEONLY:
                return jobBuilder().start(liveStep(replicationContext).build()).build();
            case SNAPSHOT:
                SimpleJobBuilder snapshot = jobBuilder().start(scanStep(replicationContext).build());
                if (shouldCompare()) {
                    snapshot.next(compareStep(replicationContext));
                }
                return snapshot.build();
            default:
                throw new IllegalArgumentException("Unknown replication mode: " + mode);
        }
    }

    @Override
    protected StandardEvaluationContext evaluationContext(RiotExecutionContext executionContext) {
        ReplicationExecutionContext replicationExecutionContext = (ReplicationExecutionContext) executionContext;
        StandardEvaluationContext evaluationContext = super.evaluationContext(executionContext);
        evaluationContext.setVariable(VARIABLE_SOURCE, replicationExecutionContext.getRedisURI());
        evaluationContext.setVariable(VARIABLE_TARGET, replicationExecutionContext.getTargetExecutionContext().getRedisURI());
        return evaluationContext;
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

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep(ReplicationExecutionContext context) {
        return step(context, STEP_SCAN, reader(context, codec));
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step(ReplicationExecutionContext context, String name,
            RedisItemReader<byte[], byte[]> reader) {
        reader.setName(name + "-reader");
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = createStep();
        step.name(name);
        step.reader(reader);
        step.writer(writer(context));
        step.processor(keyValueProcessor(context));
        if (log.isDebugEnabled()) {
            step.addWriteListener(new KeyValueWriteListener(log));
        }
        return step;
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep(ReplicationExecutionContext context) {
        checkKeyspaceNotificationsConfig(context);
        RedisItemReader<byte[], byte[]> reader = reader(context, codec);
        reader.setMode(Mode.LIVE);
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = step(context, STEP_LIVE, reader);
        step.flushingInterval(readerOptions.getFlushingInterval());
        step.idleTimeout(readerOptions.getIdleTimeout());
        return step;
    }

    private void checkKeyspaceNotificationsConfig(ReplicationExecutionContext context) {
        try {
            String config = context.getRedisConnection().sync().configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
                    .getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
            if (!config.contains("K")) {
                log.error("Keyspace notifications not property configured ({}={}). Make sure it contains at least \"K\".",
                        CONFIG_NOTIFY_KEYSPACE_EVENTS, config);
            }
        } catch (RedisException e) {
            // CONFIG command might not be available. Ignore.
        }
    }

    private Step compareStep(ReplicationExecutionContext context) {
        KeyComparisonItemReader reader = comparisonReader(context);
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

    private KeyComparisonItemReader comparisonReader(ReplicationExecutionContext context) {
        RedisItemReader<String, String> leftReader = reader(context, StringCodec.UTF8);
        AbstractRedisClient targetRedisClient = context.getTargetExecutionContext().getRedisClient();
        KeyValueItemProcessor<String, String> rightReader = new KeyValueItemProcessor<>(targetRedisClient, StringCodec.UTF8);
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

    private ItemWriter<KeyValue<byte[]>> writer(ReplicationExecutionContext context) {
        AbstractRedisClient targetRedisClient = context.getTargetExecutionContext().getRedisClient();
        RedisItemWriter<byte[], byte[]> writer = new RedisItemWriter<>(targetRedisClient, codec);
        targetWriterOptions.configure(writer);
        writer.setValueType(valueType);
        return writer;
    }

}
