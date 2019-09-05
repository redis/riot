package com.redislabs.riot;

import java.security.Security;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.riot.cli.HelpAwareCommand;
import com.redislabs.riot.cli.ManifestVersionProvider;
import com.redislabs.riot.cli.db.DatabaseConnector;
import com.redislabs.riot.cli.file.FileConnector;
import com.redislabs.riot.cli.file.RangeConverter;
import com.redislabs.riot.cli.generator.GeneratorConnector;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;
import com.redislabs.riot.cli.redis.RedisConnector;
import com.redislabs.riot.cli.test.TestConnector;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "riot", subcommands = { FileConnector.class, DatabaseConnector.class, RedisConnector.class,
		GeneratorConnector.class,
		TestConnector.class }, versionProvider = ManifestVersionProvider.class, commandListHeading = "Connectors:%n", synopsisSubcommandLabel = "[CONNECTOR]")
public class Riot extends HelpAwareCommand {

	@Spec
	private CommandSpec spec;

	private final static String DNS_CACHE_TTL = "networkaddress.cache.ttl";
	private final static String DNS_CACHE_NEGATIVE_TTL = "networkaddress.cache.negative.ttl";
//	private final static String SSL_PREFIX = "javax.net.ssl.";
//	private final static String SSL_TRUST_STORE = SSL_PREFIX + "trustStore";
//	private final static String SSL_TRUST_STORE_TYPE = SSL_PREFIX + "trustStoreType";
//	private final static String SSL_TRUST_STORE_PASSWORD = SSL_PREFIX + "trustStorePassword";
//	private final static String SSL_KEY_STORE = SSL_PREFIX + "keyStore";
//	private final static String SSL_KEY_STORE_TYPE = SSL_PREFIX + "keyStoreType";
//	private final static String SSL_KEY_STORE_PASSWORD = SSL_PREFIX + "keyStorePassword";

	private static final String ROOT_LOGGER = "";

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit")
	private boolean versionRequested;
	@Option(names = "--completion-script", hidden = true)
	private boolean completionScript;
	@Option(names = { "-d", "--debug" }, description = "Enable verbose logging")
	private boolean verbose;
	@Option(names = { "-q", "--quiet" }, description = "Disable all logging")
	private boolean quiet;
	@Option(names = "--dns-ttl", description = "DNS cache TTL", paramLabel = "<seconds>")
	private int dnsTtl = 0;
	@Option(names = "--dns-negative-ttl", description = "DNS cache negative TTL", paramLabel = "<seconds>")
	private int dnsNegativeTtl = 0;
	@ArgGroup(exclusive = false, heading = "Redis connection options%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();

	public static void main(String[] args) {
		new Riot().execute(args);
	}

	public RedisConnectionOptions redis() {
		return redis;
	}

	private void execute(String[] args) {
		CommandLine commandLine = new CommandLine(this).registerConverter(Range.class, new RangeConverter())
				.setCaseInsensitiveEnumValuesAllowed(true);
		ParseResult parseResult = commandLine.parseArgs(args);
		configureLogging();
		configureDns();
		int exitCode = commandLine.getExecutionStrategy().execute(parseResult);
		System.exit(exitCode);

	}

	private void configureDns() {
		org.slf4j.Logger log = LoggerFactory.getLogger(Riot.class);
		log.debug("Setting {}={}", DNS_CACHE_TTL, dnsTtl);
		Security.setProperty(DNS_CACHE_TTL, String.valueOf(dnsTtl));
		log.debug("Setting {}={}", DNS_CACHE_NEGATIVE_TTL, dnsNegativeTtl);
		Security.setProperty(DNS_CACHE_NEGATIVE_TTL, String.valueOf(dnsNegativeTtl));
	}

	private void configureLogging() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(new OneLineLogFormat(verbose));
		activeLogger.addHandler(handler);
		Logger.getLogger(ROOT_LOGGER).setLevel(quiet ? Level.OFF : (verbose ? Level.INFO : Level.SEVERE));
		Logger.getLogger(Riot.class.getPackage().getName())
				.setLevel(quiet ? Level.OFF : (verbose ? Level.FINEST : Level.INFO));
	}

	@Override
	public void run() {
		if (completionScript) {
			System.out.println(AutoComplete.bash(spec.name(), new CommandLine(new Riot())));
		} else {
			super.run();
		}
	}

}
