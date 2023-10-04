package com.redis.riot.core;

import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.KeyValueOperator;
import com.redis.riot.core.function.LongExpressionFunction;
import com.redis.riot.core.function.StreamMessageIdDropOperator;
import com.redis.riot.core.function.StringKeyValueFunction;
import com.redis.riot.core.function.ToStringKeyValueFunction;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.KeyValueItemReader;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractJobRunnable {

    private RedisReaderOptions readerOptions = new RedisReaderOptions();

    private KeyFilterOptions keyFilterOptions = new KeyFilterOptions();

    private KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();

    public void setKeyFilterOptions(KeyFilterOptions keyFilterOptions) {
        this.keyFilterOptions = keyFilterOptions;
    }

    public void setReaderOptions(RedisReaderOptions readerOptions) {
        this.readerOptions = readerOptions;
    }

    public void setProcessorOptions(KeyValueProcessorOptions options) {
        this.processorOptions = options;
    }

    protected <K> ItemProcessor<KeyValue<K>, KeyValue<K>> processor(RedisCodec<K, ?> codec, RiotContext context) {
        ToStringKeyValueFunction<K> code = new ToStringKeyValueFunction<>(codec);
        StringKeyValueFunction<K> decode = new StringKeyValueFunction<>(codec);
        return new FunctionItemProcessor<>(code.andThen(function(context.getEvaluationContext())).andThen(decode));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Function<KeyValue<String>, KeyValue<String>> function(EvaluationContext context) {
        KeyValueOperator operator = new KeyValueOperator();
        if (processorOptions.getKeyExpression() != null) {
            operator.setKeyFunction(ExpressionFunction.of(context, processorOptions.getKeyExpression()));
        }
        if (processorOptions.isDropTtl()) {
            operator.setTtlFunction(t -> 0);
        } else {
            if (processorOptions.getTtlExpression() != null) {
                operator.setTtlFunction(new LongExpressionFunction<>(context, processorOptions.getTtlExpression()));
            }
        }
        if (processorOptions.isDropStreamMessageId()) {
            operator.setValueFunction((Function) new StreamMessageIdDropOperator());
        }
        if (processorOptions.getTypeExpression() != null) {
            Function<KeyValue<String>, String> function = ExpressionFunction.of(context, processorOptions.getTypeExpression());
            operator.setTypeFunction(function.andThen(DataType::of));
        }
        return operator;
    }

    protected <K, V> void configureReader(RedisItemReader<K, V, ?> reader, RedisContext context) {
        reader.setChunkSize(readerOptions.getChunkSize());
        reader.setDatabase(context.getUri().getDatabase());
        reader.setKeyProcessor(keyFilteringProcessor(reader.getCodec()));
        reader.setKeyPattern(readerOptions.getKeyPattern());
        reader.setKeyType(readerOptions.getKeyType());
        reader.setFlushInterval(readerOptions.getFlushInterval());
        reader.setIdleTimeout(readerOptions.getIdleTimeout());
        if (reader instanceof KeyValueItemReader) {
            KeyValueItemReader<?, ?> keyValueReader = (KeyValueItemReader<?, ?>) reader;
            keyValueReader.setMemoryUsageLimit(readerOptions.getMemoryUsageLimit());
            keyValueReader.setMemoryUsageSamples(readerOptions.getMemoryUsageSamples());
            keyValueReader.setPoolSize(readerOptions.getPoolSize());
        }
        reader.setNotificationQueueCapacity(readerOptions.getNotificationQueueCapacity());
        reader.setOrderingStrategy(readerOptions.getOrderingStrategy());
        reader.setPollTimeout(readerOptions.getPollTimeout());
        reader.setQueueCapacity(readerOptions.getQueueCapacity());
        reader.setReadFrom(readerOptions.getReadFrom());
        reader.setScanCount(readerOptions.getScanCount());
        reader.setThreads(readerOptions.getThreads());
    }

    public <K> ItemProcessor<K, K> keyFilteringProcessor(RedisCodec<K, ?> codec) {
        Predicate<K> predicate = RiotUtils.keyFilterPredicate(codec, keyFilterOptions);
        if (predicate == null) {
            return null;
        }
        return new PredicateItemProcessor<>(predicate);
    }

}
