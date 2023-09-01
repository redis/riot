package com.redis.riot.core;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.MergePolicy;
import com.redis.spring.batch.RedisItemWriter.StreamIdPolicy;
import com.redis.spring.batch.RedisItemWriter.TtlPolicy;

public class RedisWriterOptions extends RedisOperationOptions {

    public static final TtlPolicy DEFAULT_TTL_POLICY = RedisItemWriter.DEFAULT_TTL_POLICY;

    public static final MergePolicy DEFAULT_MERGE_POLICY = RedisItemWriter.DEFAULT_MERGE_POLICY;

    public static final StreamIdPolicy DEFAULT_STREAM_ID_POLICY = RedisItemWriter.DEFAULT_STREAM_ID_POLICY;

    private TtlPolicy ttlPolicy = DEFAULT_TTL_POLICY;

    private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;

    private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

    public TtlPolicy getTtlPolicy() {
        return ttlPolicy;
    }

    public void setTtlPolicy(TtlPolicy ttlPolicy) {
        this.ttlPolicy = ttlPolicy;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(MergePolicy mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    public StreamIdPolicy getStreamIdPolicy() {
        return streamIdPolicy;
    }

    public void setStreamIdPolicy(StreamIdPolicy streamIdPolicy) {
        this.streamIdPolicy = streamIdPolicy;
    }

    public void configure(RedisItemWriter<?, ?> writer) {
        super.configure(writer);
        writer.setMergePolicy(mergePolicy);
        writer.setStreamIdPolicy(streamIdPolicy);
        writer.setTtlPolicy(ttlPolicy);
    }

}
