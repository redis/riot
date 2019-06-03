package com.redislabs.riot.cli.out;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.redislabs.riot.cli.AbstractCommand;

import picocli.CommandLine.Command;

@Component
@Command(name = "export", description = "Export from Redis", subcommands = {  })
public class ExportCommand extends AbstractCommand<Map<String, Object>, Map<String, Object>> {

}
