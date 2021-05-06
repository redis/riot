package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractFlushingTransferCommand;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RedisReaderOptions;
import com.redislabs.riot.RiotStepBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.*;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public abstract class AbstractTargetCommand extends AbstractFlushingTransferCommand {

    private static final String ASCII_COMPARE_MESSAGE_FORMAT = ">%,d T%,d ≠%,d ⧗%,d <%,d";
    private static final String COLORFUL_COMPARE_MESSAGE_FORMAT = "\u001b[31m>%,d \u001b[33mT%,d \u001b[35m≠%,d \u001b[36m⧗%,d \u001b[34m<%,d\u001b[0m";

    @CommandLine.ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    protected RedisOptions targetRedisOptions = new RedisOptions();
    @CommandLine.ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    protected RedisReaderOptions readerOptions = new RedisReaderOptions();
    @CommandLine.Mixin
    private CompareOptions compareOptions = new CompareOptions();

    protected void initialMax(RiotStepBuilder<?, ?> step) {
        step.initialMax(readerOptions.initialMaxSupplier(getRedisOptions()));
    }

    protected Flow verificationFlow(StepBuilderFactory stepBuilderFactory) {
        KeyValueItemReader<String, DataStructure<String>> sourceReader = dataStructureReader();
        log.debug("Creating key comparator with TTL tolerance of {} seconds", compareOptions.getTtlTolerance());
        DataStructureValueReader<String, String> targetValueReader = targetDataStructureValueReader();
        KeyComparisonResultCounter<String> counter = new KeyComparisonResultCounter<>();
        KeyComparisonItemWriter.KeyComparisonItemWriterBuilder writerBuilder = KeyComparisonItemWriter.valueReader(targetValueReader);
        writerBuilder.resultHandler(counter);
        if (compareOptions.isShowDiffs()) {
            writerBuilder.resultHandler(this::log);
        }
        writerBuilder.ttlTolerance(compareOptions.getTtlToleranceDuration());
        KeyComparisonItemWriter<String> writer = writerBuilder.build();
        StepBuilder verificationStepBuilder = stepBuilderFactory.get("verification-step");
        RiotStepBuilder<DataStructure<String>, DataStructure<String>> stepBuilder = riotStep(verificationStepBuilder, "Verifying");
        initialMax(stepBuilder);
        stepBuilder.reader(sourceReader).writer(writer);
        stepBuilder.extraMessage(() -> extraMessage(counter));
        SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = stepBuilder.build();
        step.listener(new StepExecutionListenerSupport() {
            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                if (counter.isOK()) {
                    log.info("Verification completed - all OK");
                    return super.afterStep(stepExecution);
                }
                try {
                    Thread.sleep(transferOptions.getProgressUpdateIntervalMillis());
                } catch (InterruptedException e) {
                    log.debug("Verification interrupted");
                    return null;
                }
                log.warn("Verification failed: identical={}, missing={}, extraneous={}, values={}, ttls={}, types={}", (Object[]) counter.get(KeyComparisonItemWriter.Result.OK, KeyComparisonItemWriter.Result.SOURCE, KeyComparisonItemWriter.Result.TARGET, KeyComparisonItemWriter.Result.VALUE, KeyComparisonItemWriter.Result.TTL, KeyComparisonItemWriter.Result.TYPE));
                return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
            }
        });
        TaskletStep verificationStep = step.build();
        return flow("verification-flow").start(verificationStep).build();
    }

    private String extraMessage(KeyComparisonResultCounter<String> counter) {
        Long[] counts = counter.get(KeyComparisonItemWriter.Result.SOURCE, KeyComparisonItemWriter.Result.TYPE, KeyComparisonItemWriter.Result.VALUE, KeyComparisonItemWriter.Result.TTL, KeyComparisonItemWriter.Result.TARGET);
        return " " + String.format(extraMessageFormat(), (Object[]) counts);
    }

    private String extraMessageFormat() {
        switch (transferOptions.getProgress()) {
            case COLOR:
                return COLORFUL_COMPARE_MESSAGE_FORMAT;
            default:
                return ASCII_COMPARE_MESSAGE_FORMAT;
        }
    }

    private void log(DataStructure<String> source, DataStructure<String> target, KeyComparisonItemWriter.Result result) {
        switch (result) {
            case OK:
                return;
            case SOURCE:
                log.warn("Key {} is missing from target", source.getKey());
                return;
            case TARGET:
                log.warn("Key {} in target but not in source", target.getKey());
                return;
            case TTL:
                log.warn("Key {} has different TTLs: source={} target={}", source.getKey(), source.getAbsoluteTTL(), target.getAbsoluteTTL());
                return;
            case TYPE:
                log.warn("Key {} has different types: source={} target={}", source.getKey(), source.getType(), target.getType());
                return;
            case VALUE:
                log.warn("Key {} has different values: source={} target={}", source.getKey(), source.getValue(), target.getValue());
                return;
        }
    }

    protected KeyValueItemReader<String, DataStructure<String>> dataStructureReader() {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            return readerOptions.configure(DataStructureItemReader.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig())).build();
        }
        return readerOptions.configure(DataStructureItemReader.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig())).build();
    }

    protected DataStructureValueReader<String, String> targetDataStructureValueReader() {
        if (targetRedisOptions.isCluster()) {
            return DataStructureValueReader.client(targetRedisOptions.redisClusterClient()).poolConfig(targetRedisOptions.poolConfig()).build();
        }
        return DataStructureValueReader.client(targetRedisOptions.redisClient()).poolConfig(targetRedisOptions.poolConfig()).build();
    }

}
