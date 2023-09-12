package com.redis.riot.core;

import static org.springframework.util.CollectionUtils.isEmpty;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.LongRange;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.codec.RedisCodec;

public class KeyFilterOptions {

    private List<String> includes;

    private List<String> excludes;

    private List<LongRange> slots;

    public KeyFilterOptions() {
    }

    private KeyFilterOptions(Builder builder) {
        this.includes = builder.includes;
        this.excludes = builder.excludes;
        this.slots = builder.slots;
    }

    public List<String> getIncludes() {
        return includes;
    }

    public void setIncludes(List<String> keyIncludes) {
        this.includes = keyIncludes;
    }

    public List<String> getExcludes() {
        return excludes;
    }

    public void setExcludes(List<String> keyExcludes) {
        this.excludes = keyExcludes;
    }

    public List<LongRange> getSlots() {
        return slots;
    }

    public void setSlots(List<LongRange> keySlots) {
        this.slots = keySlots;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private List<String> includes;

        private List<String> excludes;

        private List<LongRange> slots;

        private Builder() {
        }

        public Builder includes(String... includes) {
            return includes(Arrays.asList(includes));
        }

        public Builder includes(List<String> includes) {
            this.includes = includes;
            return this;
        }

        public Builder excludes(String... excludes) {
            return excludes(Arrays.asList(excludes));
        }

        public Builder excludes(List<String> excludes) {
            this.excludes = excludes;
            return this;
        }

        public Builder slots(LongRange... slots) {
            return slots(Arrays.asList(slots));
        }

        public Builder slots(List<LongRange> slots) {
            this.slots = slots;
            return this;
        }

        public KeyFilterOptions build() {
            return new KeyFilterOptions(this);
        }

    }

    public <K> Predicate<K> predicate(RedisCodec<K, ?> codec) {
        Function<K, String> toString = CodecUtils.toStringKeyFunction(codec);
        Predicate<String> stringGlobPredicate = globPredicate();
        Predicate<K> globPredicate = Predicates.map(toString, stringGlobPredicate);
        if (isEmpty(slots)) {
            return globPredicate;
        }
        return slotsPredicate(codec).and(globPredicate);
    }

    private <K> Predicate<K> slotsPredicate(RedisCodec<K, ?> codec) {
        return Predicates.slots(codec, slots);
    }

    private Predicate<String> globPredicate() {
        if (isEmpty(includes)) {
            return globPredicate(excludes).negate();
        }
        if (isEmpty(excludes)) {
            return globPredicate(includes);
        }
        return globPredicate(includes).and(globPredicate(excludes).negate());
    }

    private Predicate<String> globPredicate(List<String> patterns) {
        if (isEmpty(patterns)) {
            return Predicates.isTrue();
        }
        return Predicates.or(patterns.stream().map(com.redis.spring.batch.util.Predicates::glob));
    }

}
