package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;

import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.", aliases = "sync")
public class Replicate extends AbstractReplicateCommand {

    public enum Type {
        STRUCT, DUMP
    }

    public static final Type DEFAULT_TYPE = Type.DUMP;
    public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;

    private static final String COMPARE_STEP_NAME = "compare";
    private static final String SCAN_TASK_NAME = "Scanning";
    private static final String LIVEONLY_TASK_NAME = "Listening";
    private static final String LIVE_TASK_NAME = "Scanning/Listening";

    @Option(names = "--type", description = "Replication type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private Type type = DEFAULT_TYPE;

    @ArgGroup(exclusive = false)
    private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

    @Option(names = "--log-keys", description = "Log keys being read and written.")
    private boolean logKeys;

    @Option(names = "--compare", description = "Compare mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
    private CompareMode compareMode = DEFAULT_COMPARE_MODE;

    @Option(names = "--struct", description = "Enable data structure-specific replication")
    public void setStruct(boolean enable) {
        this.type = enable ? Type.STRUCT : Type.DUMP;
    }

    @Override
    protected boolean isQuickCompare() {
        return compareMode == CompareMode.QUICK;
    }

    @Override
    protected Job job() {
        List<Step<?, ?>> steps = new ArrayList<>();
        Step<KeyValue<byte[]>, KeyValue<byte[]>> step = step();
        steps.add(step);
        if (shouldCompare()) {
            steps.add(compareStep().name(COMPARE_STEP_NAME));
        }
        return job(steps);
    }

    @Override
    protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
        super.configureTargetRedisWriter(writer);
        log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
        targetRedisWriterArgs.configure(writer);
    }

    protected Step<KeyValue<byte[]>, KeyValue<byte[]>> step() {
        RedisItemReader<byte[], byte[]> reader = reader();
        configureSourceRedisReader(reader);
        RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = writer();
        configureTargetRedisWriter(writer);
        Step<KeyValue<byte[]>, KeyValue<byte[]>> step = step(reader, writer);
        step.processor(processor());
        step.taskName(taskName(reader));
        if (logKeys) {
            log.info("Adding key logger");
            step.writeListener(new ReplicateWriteLogger<>(log, reader.getCodec()));
            ReplicateReadLogger<byte[]> readLogger = new ReplicateReadLogger<>(log, reader.getCodec());
            reader.addItemReadListener(readLogger);
            reader.addItemWriteListener(readLogger);
        }
        return step;
    }

    private boolean shouldCompare() {
        return compareMode != CompareMode.NONE && !getJobArgs().isDryRun();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisItemReader<byte[], byte[]> reader() {
        if (isStruct()) {
            log.info("Creating Redis data-structure reader");
            return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
        }
        log.info("Creating Redis dump reader");
        return (RedisItemReader) RedisItemReader.dump();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer() {
        if (isStruct()) {
            log.info("Creating Redis data-structure writer");
            return RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
        }
        log.info("Creating Redis dump writer");
        return (RedisItemWriter) RedisItemWriter.dump();
    }

    @Override
    protected boolean isStruct() {
        return type == Type.STRUCT;
    }

    private String taskName(RedisItemReader<?, ?> reader) {
        switch (reader.getMode()) {
            case SCAN:
                return SCAN_TASK_NAME;
            case LIVEONLY:
                return LIVEONLY_TASK_NAME;
            default:
                return LIVE_TASK_NAME;
        }
    }

    @Override
    protected KeyComparisonItemReader<byte[], byte[]> compareReader() {
        KeyComparisonItemReader<byte[], byte[]> reader = super.compareReader();
        reader.setProcessor(processor());
        return reader;
    }

    public RedisWriterArgs getTargetRedisWriterArgs() {
        return targetRedisWriterArgs;
    }

    public void setTargetRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
        this.targetRedisWriterArgs = redisWriterArgs;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isLogKeys() {
        return logKeys;
    }

    public void setLogKeys(boolean enable) {
        this.logKeys = enable;
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

}
