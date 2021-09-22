package com.redis.riot.redis;

import com.redis.riot.*;
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
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.KeyComparisonItemWriter;
import org.springframework.batch.item.redis.support.KeyComparisonMismatchPrinter;
import org.springframework.batch.item.redis.support.KeyComparisonResultCounter;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public abstract class AbstractTargetCommand extends AbstractFlushingTransferCommand {

    private static final String ASCII_COMPARE_MESSAGE_FORMAT = ">%,d T%,d ≠%,d ⧗%,d <%,d";
    private static final String COLORFUL_COMPARE_MESSAGE_FORMAT = "\u001b[31m>%,d \u001b[33mT%,d \u001b[35m≠%,d \u001b[36m⧗%,d\u001b[0m";

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
        KeyValueItemReader<DataStructure> sourceReader = dataStructureReader();
        log.debug("Creating key comparator with TTL tolerance of {} seconds", compareOptions.getTtlTolerance());
        DataStructureValueReader targetValueReader = targetDataStructureValueReader();
        KeyComparisonResultCounter counter = new KeyComparisonResultCounter();
        KeyComparisonItemWriter.KeyComparisonItemWriterBuilder writerBuilder = KeyComparisonItemWriter.valueReader(targetValueReader);
        writerBuilder.resultHandler(counter);
        if (compareOptions.isShowDiffs()) {
            writerBuilder.resultHandler(new KeyComparisonMismatchPrinter());
        }
        writerBuilder.ttlTolerance(compareOptions.getTtlToleranceDuration());
        KeyComparisonItemWriter writer = writerBuilder.build();
        StepBuilder verificationStepBuilder = stepBuilderFactory.get("verification-step");
        RiotStepBuilder<DataStructure, DataStructure> stepBuilder = riotStep(verificationStepBuilder, "Verifying");
        initialMax(stepBuilder);
        stepBuilder.reader(sourceReader).writer(writer);
        stepBuilder.extraMessage(() -> extraMessage(counter));
        SimpleStepBuilder<DataStructure, DataStructure> step = stepBuilder.build();
        step.listener(new StepExecutionListenerSupport() {
            @SuppressWarnings("NullableProblems")
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
                log.warn("Verification failed: OK={} Missing={} Values={} TTLs={} Types={}", (Object[]) counter.get(KeyComparisonItemWriter.Status.OK, KeyComparisonItemWriter.Status.MISSING, KeyComparisonItemWriter.Status.VALUE, KeyComparisonItemWriter.Status.TTL, KeyComparisonItemWriter.Status.TYPE));
                return new ExitStatus(ExitStatus.FAILED.getExitCode(), "Verification failed");
            }
        });
        TaskletStep verificationStep = step.build();
        return flow("verification-flow").start(verificationStep).build();
    }

    private String extraMessage(KeyComparisonResultCounter counter) {
        Long[] counts = counter.get(KeyComparisonItemWriter.Status.MISSING, KeyComparisonItemWriter.Status.TYPE, KeyComparisonItemWriter.Status.VALUE, KeyComparisonItemWriter.Status.TTL);
        return " " + String.format(extraMessageFormat(), (Object[]) counts);
    }

    private String extraMessageFormat() {
        if (transferOptions.getProgress() == TransferOptions.Progress.COLOR) {
            return COLORFUL_COMPARE_MESSAGE_FORMAT;
        }
        return ASCII_COMPARE_MESSAGE_FORMAT;
    }

    protected KeyValueItemReader<DataStructure> dataStructureReader() {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            return readerOptions.configure(DataStructureItemReader.client(redisOptions.clusterClient()).poolConfig(redisOptions.poolConfig())).build();
        }
        return readerOptions.configure(DataStructureItemReader.client(redisOptions.client()).poolConfig(redisOptions.poolConfig())).build();
    }

    protected DataStructureValueReader targetDataStructureValueReader() {
        if (targetRedisOptions.isCluster()) {
            return DataStructureValueReader.client(targetRedisOptions.clusterClient()).poolConfig(targetRedisOptions.poolConfig()).build();
        }
        return DataStructureValueReader.client(targetRedisOptions.client()).poolConfig(targetRedisOptions.poolConfig()).build();
    }

}
