package com.redis.riot.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.util.PredicateItemProcessor;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.operation.CompositeOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public abstract class AbstractMapImport extends AbstractJobExecutable {

    private MapProcessorOptions processorOptions = new MapProcessorOptions();

    private RedisOperationOptions operationOptions = new RedisOperationOptions();

    private List<Operation<String, String, Map<String, Object>>> operations;

    protected AbstractMapImport(AbstractRedisClient client) {
        super(client);
    }

    public void setProcessorOptions(MapProcessorOptions options) {
        this.processorOptions = options;
    }

    public void setOperations(List<Operation<String, String, Map<String, Object>>> operations) {
        this.operations = operations;
    }

    public void setOperationOptions(RedisOperationOptions operationOptions) {
        this.operationOptions = operationOptions;
    }

    protected OperationItemWriter<String, String, Map<String, Object>> writer() {
        OperationItemWriter<String, String, Map<String, Object>> writer = new OperationItemWriter<>(client, StringCodec.UTF8);
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

    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        EvaluationContext context = evaluationContext();
        if (!CollectionUtils.isEmpty(processorOptions.getExpressions())) {
            processors.add(new FunctionItemProcessor<>(SpelUtils.mapOperator(context, processorOptions.getExpressions())));
        }
        if (processorOptions.getFilter() != null) {
            Predicate<Map<String, Object>> predicate = SpelUtils.predicate(context, processorOptions.getFilter());
            processors.add(new PredicateItemProcessor<>(predicate));
        }
        return processor(processors);
    }

    protected StandardEvaluationContext evaluationContext() {
        StandardEvaluationContext context = processorOptions.getEvaluationContextOptions().evaluationContext();
        context.addPropertyAccessor(new QuietMapAccessor());
        return context;
    }

    /**
     * {@link org.springframework.context.expression.MapAccessor} that always returns true for canRead and does not throw
     * AccessExceptions
     *
     * @author Julien Ruaux
     */
    private static class QuietMapAccessor extends MapAccessor {

        @Override
        public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
            return true;
        }

        @Override
        public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {
            try {
                return super.read(context, target, name);
            } catch (AccessException e) {
                return new TypedValue(null);
            }
        }

    }

}
