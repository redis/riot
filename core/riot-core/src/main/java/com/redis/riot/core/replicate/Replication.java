package com.redis.riot.core.replicate;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.KeyValueOperatorOptions;
import com.redis.riot.core.RedisReaderOptions;
import com.redis.riot.core.RedisWriterOptions;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Mode;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

public class Replication extends AbstractExport {

    public static final String CONFIG_NOTIFY_KEYSPACE_EVENTS = "notify-keyspace-events";

    public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

    private static final Logger log = LoggerFactory.getLogger(Replication.class);

    private final AbstractRedisClient targetClient;

    private ReplicationMode mode = ReplicationMode.SNAPSHOT;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

    private boolean noVerify;

    private Duration ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE;

    private RedisReaderOptions targetReaderOptions = new RedisReaderOptions();

    private RedisWriterOptions targetWriterOptions = new RedisWriterOptions();

    private KeyValueOperatorOptions processorOptions = new KeyValueOperatorOptions();

    public Replication(AbstractRedisClient client, AbstractRedisClient targetClient) {
        super(client);
        this.targetClient = targetClient;
    }

    public void setProcessorOptions(KeyValueOperatorOptions options) {
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

    public void setTargetReaderOptions(RedisReaderOptions options) {
        this.targetReaderOptions = options;
    }

    public void setTargetWriterOptions(RedisWriterOptions options) {
        this.targetWriterOptions = options;
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
        return !noVerify && !getStepOptions().isDryRun() && processorOptions.isEmpty();
    }

    private SimpleStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> scanStep() {
        return step("scan", reader(ByteArrayCodec.INSTANCE));
    }

    private SimpleStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step(String name, RedisItemReader<byte[], byte[]> reader) {
        reader.setName(name + "-reader");
        StepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = step(name).reader(reader).writer(writer());
        step.processor(processor());
        if (log.isDebugEnabled()) {
            step.listeners(new KeyValueWriteListener(log));
        }
        return step.build();
    }

    private FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> liveStep() {
        checkKeyspaceNotificationsConfig();
        RedisItemReader<byte[], byte[]> reader = reader(ByteArrayCodec.INSTANCE);
        reader.setMode(Mode.LIVE);
        SimpleStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> riotStep = step("live", reader);
        FlushingStepBuilder<KeyValue<byte[]>, KeyValue<byte[]>> step = new FlushingStepBuilder<>(riotStep);
        step.interval(getRedisReaderOptions().getFlushingInterval());
        step.idleTimeout(getRedisReaderOptions().getIdleTimeout());
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

    private TaskletStep compareStep() {
        KeyComparisonItemReader reader = new KeyComparisonItemReader(reader(StringCodec.UTF8),
                reader(targetClient, StringCodec.UTF8, targetReaderOptions));
        reader.setTtlTolerance(ttlTolerance);
        reader.setName("compare-reader");
        KeyComparisonStatusCountItemWriter writer = new KeyComparisonStatusCountItemWriter();
        return step("compare").reader(reader).writer(writer).build().build();
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
