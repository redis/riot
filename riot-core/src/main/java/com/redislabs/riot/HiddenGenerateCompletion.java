package com.redislabs.riot;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine.Command;

@Command(hidden = true, name = "generate-completion", usageHelpAutoWidth = true)
public class HiddenGenerateCompletion extends GenerateCompletion {

}
