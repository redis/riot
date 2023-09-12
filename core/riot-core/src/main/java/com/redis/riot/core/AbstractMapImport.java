package com.redis.riot.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;

import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.operation.CompositeOperation;

import io.lettuce.core.codec.StringCodec;

public abstract class AbstractMapImport extends AbstractJobExecutable {

    private ProcessorOptions processorOptions = new ProcessorOptions();

    private RedisOperationOptions operationOptions = new RedisOperationOptions();

    private List<Operation<String, String, Map<String, Object>>> operations;

    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RiotExecutionContext context) {
        return processorOptions.processor(evaluationContext(context));
    }

    public void setProcessorOptions(ProcessorOptions options) {
        this.processorOptions = options;
    }

    @SuppressWarnings("unchecked")
    public void setOperations(Operation<String, String, Map<String, Object>>... operations) {
        setOperations(Arrays.asList(operations));
    }

    public void setOperations(List<Operation<String, String, Map<String, Object>>> operations) {
        this.operations = operations;
    }

    public void setOperationOptions(RedisOperationOptions operationOptions) {
        this.operationOptions = operationOptions;
    }

    protected OperationItemWriter<String, String, Map<String, Object>> writer(RiotExecutionContext context) {
        OperationItemWriter<String, String, Map<String, Object>> writer = new OperationItemWriter<>(context.getRedisClient(),
                StringCodec.UTF8);
        writer.setOperation(operation());
        operationOptions.configure(writer);
        return writer;
    }

    private Operation<String, String, Map<String, Object>> operation() {
        Assert.notEmpty(operations, "No operation specified");
        if (operations.size() == 1) {
            return operations.get(0);
        }
        CompositeOperation<String, String, Map<String, Object>> operation = new CompositeOperation<>();
        operation.delegates(operations);
        return operation;
    }

}
