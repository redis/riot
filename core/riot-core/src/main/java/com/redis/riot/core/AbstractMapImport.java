package com.redis.riot.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractMapImport extends AbstractJobRunnable {

    private ProcessorOptions processorOptions = new ProcessorOptions();

    private RedisWriterOptions writerOptions = new RedisWriterOptions();

    private List<WriteOperation<String, String, Map<String, Object>>> operations;

    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RiotContext context) {
        return processorOptions.processor(context.getEvaluationContext());
    }

    public void setProcessorOptions(ProcessorOptions options) {
        this.processorOptions = options;
    }

    @SuppressWarnings("unchecked")
    public void setOperations(WriteOperation<String, String, Map<String, Object>>... operations) {
        setOperations(Arrays.asList(operations));
    }

    public void setOperations(List<WriteOperation<String, String, Map<String, Object>>> operations) {
        this.operations = operations;
    }

    public void setWriterOptions(RedisWriterOptions operationOptions) {
        this.writerOptions = operationOptions;
    }

    protected ItemWriter<Map<String, Object>> writer(RiotContext context) {
        Assert.notEmpty(operations, "No operation specified");
        AbstractRedisClient client = context.getRedisContext().getClient();
        return RiotUtils.writer(operations.stream().map(o -> writer(client, o)).collect(Collectors.toList()));
    }

    private <T> ItemWriter<T> writer(AbstractRedisClient client, WriteOperation<String, String, T> operation) {
        OperationItemWriter<String, String, T> writer = RedisItemWriter.operation(client, operation);
        return writer(writer, writerOptions);
    }

}
