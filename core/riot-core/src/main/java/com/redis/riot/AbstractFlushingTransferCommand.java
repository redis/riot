package com.redis.riot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractFlushingTransferCommand extends AbstractTransferCommand {

    @CommandLine.Mixin
    protected FlushingTransferOptions flushingTransferOptions = new FlushingTransferOptions();

}
