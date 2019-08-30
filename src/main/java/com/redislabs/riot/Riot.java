package com.redislabs.riot;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Security;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.riot.cli.PingCommand;
import com.redislabs.riot.cli.ExportParentCommand;
import com.redislabs.riot.cli.HelpAwareCommand;
import com.redislabs.riot.cli.ImportParentCommand;
import com.redislabs.riot.cli.ManifestVersionProvider;
import com.redislabs.riot.cli.file.RangeConverter;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;

@Command(name = "riot", subcommands = { ImportParentCommand.class, ExportParentCommand.class,
		PingCommand.class }, versionProvider = ManifestVersionProvider.class)
public class Riot extends HelpAwareCommand {

	private final static String DNS_CACHE_TTL = "networkaddress.cache.ttl";
	private final static String DNS_CACHE_NEGATIVE_TTL = "networkaddress.cache.negative.ttl";
	private final static String SSL_PREFIX = "javax.net.ssl.";
	private final static String SSL_TRUST_STORE = SSL_PREFIX + "trustStore";
	private final static String SSL_TRUST_STORE_TYPE = SSL_PREFIX + "trustStoreType";
	private final static String SSL_TRUST_STORE_PASSWORD = SSL_PREFIX + "trustStorePassword";
	private final static String SSL_KEY_STORE = SSL_PREFIX + "keyStore";
	private final static String SSL_KEY_STORE_TYPE = SSL_PREFIX + "keyStoreType";
	private final static String SSL_KEY_STORE_PASSWORD = SSL_PREFIX + "keyStorePassword";

	private List<Logger> activeLoggers = new ArrayList<>();

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit")
	private boolean version;
	@Option(names = "--completion-script", hidden = true)
	private boolean completionScript;
	@Option(names = { "-v", "--verbose" }, description = "Enable verbose logging")
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

	public RedisConnectionOptions getRedis() {
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

	private Logger getLogger(String name) {
		Logger logger = Logger.getLogger(name);
		activeLoggers.add(logger);
		return logger;
	}

	private void configureLogging() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = getLogger("");
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(new OneLineLogFormat());
		activeLogger.addHandler(handler);
		getLogger("").setLevel(quiet ? Level.OFF : (verbose ? Level.INFO : Level.SEVERE));
		getLogger(Riot.class.getPackage().getName())
				.setLevel(quiet ? Level.OFF : (verbose ? Level.FINEST : Level.INFO));
	}

	class OneLineLogFormat extends Formatter {

		private DateTimeFormatter d = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
				.appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).optionalStart().appendLiteral(':')
				.appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart()
				.appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true).toFormatter();
		private ZoneId offset = ZoneOffset.systemDefault();

		@Override
		public String format(LogRecord record) {
			String message = formatMessage(record);
			ZonedDateTime time = Instant.ofEpochMilli(record.getMillis()).atZone(offset);
			if (record.getThrown() == null) {
				return String.format("%s\t%s%n", time.format(d), message);
			}
			if (verbose) {
				return String.format("%s\t%s%n%s%n", time.format(d), message, stackTrace(record));
			}
			return String.format("%s\t%s: %s%n", time.format(d), message, record.getThrown().getMessage());
		}

		private String stackTrace(LogRecord record) {
			StringWriter sw = new StringWriter(4096);
			PrintWriter pw = new PrintWriter(sw);
			record.getThrown().printStackTrace(pw);
			return sw.toString();
		}
	}

	@Override
	public void run() {
		if (completionScript) {
			System.out.println(AutoComplete.bash("riot", new CommandLine(new Riot())));
		} else {
			super.run();
		}
	}

}
