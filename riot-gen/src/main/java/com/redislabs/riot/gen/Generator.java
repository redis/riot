package com.redislabs.riot.gen;

public interface Generator<T> {

    T next(long index);
}
