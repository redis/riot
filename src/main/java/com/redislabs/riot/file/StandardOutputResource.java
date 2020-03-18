package com.redislabs.riot.file;

public class StandardOutputResource extends OutputStreamResource {
    public StandardOutputResource() {
        super(System.out, "stdout");
    }
}