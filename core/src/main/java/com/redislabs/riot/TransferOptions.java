package com.redislabs.riot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferOptions {

    public enum Progress {
        ASCII, COLOR, BW, NONE
    }

    @Builder.Default
    @CommandLine.Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    private Progress progress = Progress.COLOR;
    @Builder.Default
    @CommandLine.Option(names = "--progress-interval", description = "Progress update interval in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<ms>", hidden = true)
    private long progressUpdateIntervalMillis = 300;
    @Builder.Default
    @CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @Builder.Default
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int chunkSize = 50;
    @Builder.Default
    @CommandLine.Option(names = "--skip-limit", description = "Max number of failed items to skip before the transfer fails (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int skipLimit = 0;

}
