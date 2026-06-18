package benchmark;

import core.LSMStorageEngine;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
public class WriteThroughputBenchmark {

    private LSMStorageEngine engine;
    private int counter;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path tempDir = Files.createTempDirectory("kvdb-benchmark");
        engine = new LSMStorageEngine(tempDir.toString());
        counter = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }

    @Benchmark
    public void putSmallValue() throws IOException {
        String key = "key-" + counter++;
        String value = "value-" + counter;
        engine.put(key.getBytes(StandardCharsets.UTF_8), value);
    }

    @Benchmark
    @Threads(10)
    public void putSmallValueConcurrent() throws IOException {
        String key = "key-" + Thread.currentThread().getId() + "-" + counter++;
        String value = "value-" + counter;
        engine.put(key.getBytes(StandardCharsets.UTF_8), value);
    }

    @Benchmark
    public void putLargeValue() throws IOException {
        String key = "key-" + counter++;
        byte[] value = new byte[1024];
        engine.put(key.getBytes(StandardCharsets.UTF_8), value);
    }
}
