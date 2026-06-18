package benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.CommandLineOptionException;

public class BenchmarkRunner {
    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        Options options;
        if (args.length > 0) {
            options = new CommandLineOptions(args);
        } else {
            options = new OptionsBuilder()
                    .include("benchmark\\..*")
                    .forks(0)
                    .warmupIterations(2)
                    .measurementIterations(3)
                    .resultFormat(org.openjdk.jmh.results.format.ResultFormatType.JSON)
                    .result("benchmark-result.json")
                    .build();
        }

        new Runner(options).run();
    }
}
