package com.redislabs.riot.file;

public enum DumpFileType {

    JSON, XML;

    public static DumpFileType of(String file) {
        String extension = FileUtils.extension(file);
        if (extension != null && extension.equalsIgnoreCase(FileUtils.EXTENSION_XML)) {
            return XML;
        }
        return JSON;
    }
}