package com.redis.riot.gen;

public interface Generator<T> {

    T next(int index);
}
