package com.redislabs.riot.cli;

import java.io.File;

import org.springframework.batch.item.support.ScriptItemProcessor;
import org.springframework.batch.item.support.builder.ScriptItemProcessorBuilder;
import org.springframework.core.io.FileSystemResource;

import picocli.CommandLine.Option;

public class ScriptProcessorOptions {

	@Option(names = "--script", description = "Use an inline script to process items", paramLabel = "<script>")
	private String script;
	@Option(names = "--script-file", description = "Use an external script to process items", paramLabel = "<file>")
	private File scriptFile;
	@Option(names = "--script-lang", description = "Script language (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private String processorScriptLanguage = "ECMAScript";

	public boolean isSet() {
		return script != null || scriptFile != null;
	}

	public <I, O> ScriptItemProcessor<I, O> processor() throws Exception {
		System.setProperty("nashorn.args", "--no-deprecation-warning");
		ScriptItemProcessorBuilder<I, O> builder = new ScriptItemProcessorBuilder<>();
		builder.language(processorScriptLanguage);
		if (script != null) {
			builder.scriptSource(script);
		}
		if (scriptFile != null) {
			builder.scriptResource(new FileSystemResource(scriptFile));
		}
		ScriptItemProcessor<I, O> processor = builder.build();
		processor.afterPropertiesSet();
		return processor;
	}

}
