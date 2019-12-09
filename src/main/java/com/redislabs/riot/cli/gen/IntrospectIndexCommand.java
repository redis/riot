package com.redislabs.riot.cli.gen;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.RediSearchUtils.IndexInfo;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.TagField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(name = "fakerize", description = "Generates Faker fields based on a RediSearch index2")
public class IntrospectIndexCommand extends HelpCommand implements Runnable {

	@SuppressWarnings("rawtypes")
	@ParentCommand
	private ImportCommand parent;
	@Spec
	private CommandSpec spec;

	@Option(names = { "-i", "--index" }, description = "Name of the RediSearch index", paramLabel = "<name>")
	private String index;

	@Override
	public void run() {
		RediSearchCommands<String, String> ft = parent.redisOptions().lettuSearchClient().connect().sync();
		IndexInfo info = RediSearchUtils.getInfo(ft.indexInfo(index));
		List<String> expressions = new ArrayList<>();
		info.fields().forEach(f -> expressions.add(f.name() + "=" + expression(f)));
		System.out.println(String.join(" ", expressions));
	}

	private String expression(Field field) {
		if (field instanceof TextField) {
			return "lorem.paragraph";
		}
		if (field instanceof TagField) {
			return "number.digits(10)";
		}
		if (field instanceof GeoField) {
			return "address.longitude.concat(',').concat(address.latitude)";
		}
		return "number.randomDouble(3,-1000,1000)";
	}
}
