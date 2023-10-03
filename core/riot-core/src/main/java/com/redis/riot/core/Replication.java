package com.redis.riot.core;

import java.io.PrintWriter;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyValueItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.KeyValueItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class Replication extends AbstractExport {

    public static final ReplicationType DEFAULT_TYPE = ReplicationType.DUMP;

    public static final ReplicationMode DEFAULT_MODE = ReplicationMode.SNAPSHOT;

    public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";

    public static final String STEP_LIVE = "live";

    public static final String STEP_SCAN = "scan";

    public static final String STEP_COMPARE = "compare";

    private static final String VARIABLE_SOURCE = "source";

    private static final String VARIABLE_TARGET = "target";

    private final PrintWriter out;

    private ReplicationMode mode = DEFAULT_MODE;

    private ReplicationType type = DEFAULT_TYPE;

    private RedisOptions targetRedisOptions = new RedisOptions();

    private ReadFrom targetReadFrom;

    private RedisWriterOptions targetWriterOptions = new RedisWriterOptions();

    private KeyComparisonOptions comparisonOptions = new KeyComparisonOptions();

    public Replication(PrintWriter out) {
        this.out = out;
    }

    public void setTargetRedisOptions(RedisOptions options) {
        this.targetRedisOptions = options;
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

    public void setType(ReplicationType type) {
        this.type = type;
    }

    @Override
    protected RiotContext createExecutionContext() {
        RiotContext baseContext = super.createExecutionContext();
        RedisContext targetRedisContext = redisContext(targetRedisOptions);
        StandardEvaluationContext evaluationContext = baseContext.getEvaluationContext();
        evaluationContext.setVariable(VARIABLE_SOURCE, baseContext.getRedisContext().getUri());
        evaluationContext.setVariable(VARIABLE_TARGET, targetRedisContext.getUri());
        return new ReplicationContext(baseContext.getRedisContext(), evaluationContext, targetRedisContext);
    }

    @Override
    protected Job job(RiotContext context) {
        Assert.isInstanceOf(ReplicationContext.class, context, "Execution context is not a replication context");
        ReplicationContext replicationContext = (ReplicationContext) context;
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

    private TaskExecutor asyncTaskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    private static FlowBuilder<SimpleFlow> flow(String name) {
        return new FlowBuilder<>(name);
    }

    private boolean shouldCompare() {
        return !comparisonOptions.isSkip() && !getStepOptions().isDryRun();
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep(ReplicationContext context) {
        return step(context, STEP_SCAN, reader(context.getRedisContext()));
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step(ReplicationContext context, String name,
            RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader) {
        reader.setName(name + "-reader");
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = createStep();
        step.name(name);
        step.reader(reader);
        step.processor(processor(ByteArrayCodec.INSTANCE, context));
        step.writer(writer(context));
        if (log.isDebugEnabled()) {
            step.addWriteListener(new KeyValueWriteListener<>(reader.getCodec(), log));
        }
        return step;
    }

    private StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep(ReplicationContext context) {
        validateRedisConfig(context);
        RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader = reader(context.getRedisContext());
        reader.setMode(ReaderMode.LIVE);
        return step(context, STEP_LIVE, reader);
    }

    private RedisItemReader<byte[], byte[], KeyValue<byte[]>> reader(RedisContext context) {
        KeyValueItemReader<byte[], byte[]> reader = reader(context.getClient());
        configureReader(reader, context);
        return reader;
    }

    private KeyValueItemReader<byte[], byte[]> reader(AbstractRedisClient client) {
        if (type == ReplicationType.STRUCT) {
            return new StructItemReader<>(client, ByteArrayCodec.INSTANCE);
        }
        return new DumpItemReader(client);
    }

    private void validateRedisConfig(ReplicationContext context) {
        try {
            String config = context.getRedisContext().getConnection().sync().configGet(CONFIG_NOTIFY_KEYSPACE_EVENTS)
                    .getOrDefault(CONFIG_NOTIFY_KEYSPACE_EVENTS, "");
            if (!config.contains("K")) {
                log.error("Keyspace notifications not property configured ({}={}). Make sure it contains at least \"K\".",
                        CONFIG_NOTIFY_KEYSPACE_EVENTS, config);
            }
        } catch (RedisException e) {
            // CONFIG command might not be available. Ignore.
        }
    }

    private Step compareStep(ReplicationContext context) {
        KeyComparisonItemReader reader = comparisonReader(context);
        KeyComparisonStatusCountItemWriter writer = new KeyComparisonStatusCountItemWriter();
        StepBuilder<KeyComparison, KeyComparison> step = createStep();
        step.name(STEP_COMPARE);
        step.reader(reader);
        step.writer(writer);
        if (comparisonOptions.isShowDiffs()) {
            step.addWriteListener(new KeyComparisonDiffLogger(out));
        }
        step.addExecutionListener(new KeyComparisonSummaryLogger(writer, out));
        return step.build();
    }

    private KeyComparisonItemReader comparisonReader(ReplicationContext context) {
        KeyValueItemReader<String, String> sourceReader = compareReader(context.getRedisContext());
        configureReader(sourceReader, context.getRedisContext());
        KeyValueItemReader<String, String> targetReader = compareReader(context.getTargetRedisContext());
        targetReader.setReadFrom(targetReadFrom);
        targetReader.setPoolSize(targetWriterOptions.getPoolSize());
        return new KeyComparisonItemReader(sourceReader, targetReader);
    }

    private KeyValueItemReader<String, String> compareReader(RedisContext context) {
        if (comparisonOptions.getMode() == KeyComparisonMode.FULL) {
            return new StructItemReader<>(context.getClient(), StringCodec.UTF8);
        }
        return new KeyTypeItemReader<>(context.getClient(), StringCodec.UTF8);
    }

    private RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer(ReplicationContext context) {
        AbstractRedisClient targetRedisClient = context.getTargetRedisContext().getClient();
        KeyValueItemWriter<byte[], byte[]> writer = writer(targetRedisClient);
        return writer(writer, targetWriterOptions);
    }

    private KeyValueItemWriter<byte[], byte[]> writer(AbstractRedisClient client) {
        if (type == ReplicationType.STRUCT) {
            return new StructItemWriter<>(client, ByteArrayCodec.INSTANCE);
        }
        return new DumpItemWriter(client);
    }

}
