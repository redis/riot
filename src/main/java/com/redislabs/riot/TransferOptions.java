package com.redislabs.riot;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TransferOptions {

    public final static int DEFAULT_THREAD_COUNT = 1;
    public final static int DEFAULT_BATCH_SIZE = 50;

    @Builder.Default
    private int threadCount = DEFAULT_THREAD_COUNT;
    @Builder.Default
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Long flushPeriod;
    private Integer maxItemCount;
}
