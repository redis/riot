package com.redislabs.riot.file;

public enum FileType {

    DELIMITED(FileUtils.EXT_CSV, FileUtils.EXT_TSV), FIXED(FileUtils.EXT_FW), JSON(FileUtils.EXT_JSON), XML(FileUtils.EXT_XML);

    private final String[] extensions;

    FileType(String... extensions) {
        this.extensions = extensions;
    }

    public String[] getExtensions() {
        return extensions;
    }
}