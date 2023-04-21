package com.redis.riot.core;

public interface Generator<T> {

    T next(int index);
}
