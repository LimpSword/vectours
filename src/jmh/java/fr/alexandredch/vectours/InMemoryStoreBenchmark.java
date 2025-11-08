package fr.alexandredch.vectours;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.base.InMemoryStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class InMemoryStoreBenchmark {

    @State(Scope.Benchmark)
    public static class SearchState {
        public InMemoryStore store;

        // Setup at the start of the benchmark trial
        @Setup(Level.Trial)
        public void setUp() {
            store = new InMemoryStore();
            for (int i = 0; i < 100000; i++) {
                store.insert(
                        "id" + i,
                        new Vector("id" + i, new double[] {(double) i, (double) i + 1, (double) i + 2}, null));
            }
        }
    }

    @State(Scope.Benchmark)
    public static class InitState {
        public InMemoryStore store;

        // Setup at the start of the benchmark trial
        @Setup(Level.Trial)
        public void setUp() {
            store = new InMemoryStore();
            for (int i = 0; i < 100000; i++) {
                store.insert(
                        "id" + i,
                        new Vector("id" + i, new double[] {(double) i, (double) i + 1, (double) i + 2}, null));
            }
            store.runTasks();
            try {
                Thread.sleep(65000); // Wait for background tasks to complete
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            store = null;

            store = new InMemoryStore();
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void search(SearchState state, Blackhole blackhole) {
        blackhole.consume(state.store.search(new double[] {5000, 5001, 5002}, 30));
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void initFromDisk(InitState state) {
        state.store.initFromDisk();
    }
}
