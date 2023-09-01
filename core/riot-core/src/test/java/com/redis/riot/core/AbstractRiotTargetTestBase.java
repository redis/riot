package com.redis.riot.core;

import org.junit.jupiter.api.TestInfo;

import com.redis.spring.batch.test.AbstractTargetTestBase;

abstract class AbstractRiotTargetTestBase extends AbstractTargetTestBase {

    protected void execute(AbstractJobExecutable executable, TestInfo info) {
        executable.setName(name(info));
        executable.execute();
    }

}
