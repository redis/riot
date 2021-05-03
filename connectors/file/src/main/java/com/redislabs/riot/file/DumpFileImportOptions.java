package com.redislabs.riot.file;

import lombok.*;
import lombok.experimental.Accessors;
import picocli.CommandLine;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class DumpFileImportOptions extends FileOptions {

    @CommandLine.Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private DumpFileType type;

    public static DumpFileImportOptionsBuilder builder() {
        return new DumpFileImportOptionsBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class DumpFileImportOptionsBuilder extends FileOptionsBuilder<DumpFileImportOptionsBuilder> {

        private DumpFileType type;

        public DumpFileImportOptions build() {
            return build(new DumpFileImportOptions(type));
        }

    }


}
