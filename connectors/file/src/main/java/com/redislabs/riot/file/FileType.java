package com.redislabs.riot.file;

import lombok.Getter;

public enum FileType {

    CSV("csv"), PSV("psv"), TSV("tsv"), FW("fw"), JSON("json"), XML("xml");

    @Getter
    private final String extension;

    FileType(String extension) {
        this.extension = extension;
    }

}