package fr.alexandredch.vectours;

import fr.alexandredch.vectours.data.SearchParameters;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.base.InMemoryStore;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class InMemoryStoreBenchmark {

    @State(Scope.Benchmark)
    public static class SearchState {
        public InMemoryStore store;

        // Setup at the start of the benchmark trial
        @Setup(Level.Iteration)
        public void setUp() {
            store = new InMemoryStore();
            store.initFromDisk();
            for (int i = 0; i < 1_000_000; i++) {
                store.insert(new Vector("id" + i, new double[] {(double) i, (double) i + 1, (double) i + 2}, null));
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            store.dropAll();
            store.shutdown();
            store = null;

            System.gc();
        }
    }

    @State(Scope.Benchmark)
    public static class InitState {
        public InMemoryStore store;

        // Setup at the start of the benchmark trial
        @Setup(Level.Iteration)
        public void setUp() {
            store = new InMemoryStore();
            store.initFromDisk();
            for (int i = 0; i < 100_000; i++) {
                store.insert(new Vector("id" + i, new double[] {(double) i, (double) i + 1, (double) i + 2}, null));
            }
            store.saveAll();
            store.shutdown();
            store = null;

            store = new InMemoryStore();
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            store.dropAll();
            store.shutdown();
            store = null;

            System.gc();
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void searchBruteforce(SearchState state, Blackhole blackhole) {
        blackhole.consume(state.store.search(new SearchParameters(new double[] {5000, 5001, 5002}, false, 30)));
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void searchIVF(SearchState state, Blackhole blackhole) {
        blackhole.consume(state.store.search(new SearchParameters(new double[] {5000, 5001, 5002}, true, 30)));
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void initFromDisk(InitState state) {
        state.store.initFromDisk();
    }
}
