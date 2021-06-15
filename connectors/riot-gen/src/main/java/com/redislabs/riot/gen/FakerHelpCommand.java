package com.redislabs.riot.gen;

import com.github.javafaker.Faker;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.IHelpCommandInitializable2;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.stream.Stream;

@Command(name = "faker-help", header = "Displays help information about Faker", synopsisHeading = "%nUsage: ", helpCommand = true)
public class FakerHelpCommand implements IHelpCommandInitializable2, Runnable {

    @CommandLine.Parameters(description = "Name of the Faker provider to show help for", paramLabel = "<name>")
    private String provider;

    private final static List<String> EXCLUDES = Arrays.asList("instance", "options");

    private PrintWriter outWriter;

    @Override
    public void run() {
        Stream<Method> methods = Arrays.stream(Faker.class.getDeclaredMethods()).filter(m -> !EXCLUDES.contains(m.getName())).filter(m -> m.getReturnType().getPackage().equals(Faker.class.getPackage()));
        if (provider == null) {
            outWriter.println("Run 'riot-gen faker-help <provider>' for documentation on a specific Faker provider:");
            methods.sorted(Comparator.comparing(Method::getName)).forEach(p -> outWriter.println("  " + p.getName() + " " + url(ClassUtils.getShortName(p.getReturnType()))));
        } else {
            Optional<Method> method = methods.filter(m -> m.getName().equals(provider)).findFirst();
            if (method.isPresent()) {
                Class<?> type = method.get().getReturnType();
                String shortName = ClassUtils.getShortName(type);
                outWriter.println("Available fakes: " + url(shortName));
                for (Method subMethod : type.getDeclaredMethods()) {
                    outWriter.println("  " + provider + "." + methodToString(subMethod));
                }
            } else {
                outWriter.println("No such field: " + provider);
            }
        }
    }

    private String url(String name) {
        return "http://dius.github.io/java-faker/apidocs/com/github/javafaker/" + name + ".html";
    }

    private String methodToString(Method method) {
        String result = method.getName();
        Parameter[] parameters = method.getParameters();
        if (parameters.length == 0) {
            return result;
        }
        result += "(";
        List<String> paramNames = new ArrayList<>();
        for (Parameter parameter : parameters) {
            paramNames.add(ClassUtils.getShortName(parameter.getType()));
        }
        result += String.join(",", paramNames);
        result += ")";
        return result;
    }

    public void init(CommandLine helpCommandLine, Help.ColorScheme colorScheme, PrintWriter out, PrintWriter err) {
        this.outWriter = notNull(out, "outWriter");
    }

    static <T> T notNull(T object, String description) {
        if (object == null) {
            throw new NullPointerException(description);
        }
        return object;
    }

}
