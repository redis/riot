package com.redis.riot.core;

import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;

class KeyFilterTests {

    @Test
    void keyFilter() {
        Predicate<String> predicate = KeyFilterOptions.builder().includes("foo*", "bar*").build().predicate(StringCodec.UTF8);
        Assertions.assertTrue(predicate.test("foobar"));
        Assertions.assertTrue(predicate.test("barfoo"));
        Assertions.assertFalse(predicate.test("key"));
    }

}
