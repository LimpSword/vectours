package fr.alexandredch.vectours;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.InMemoryStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class InMemoryStoreBenchmark {

    @State(Scope.Benchmark)
    public static class InMemoryStoreState {
        public InMemoryStore store;

        // Setup at the start of the benchmark trial
        @Setup(Level.Trial)
        public void setUp() {
            store = new InMemoryStore();
            for (int i = 0; i < 100000; i++) {
                store.insert("id" + i, new Vector("id" + i, new float[]{(float) i, (float) i + 1, (float) i + 2}, null));
            }
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    public void search(InMemoryStoreState state, Blackhole blackhole) {
        blackhole.consume(state.store.search(new float[]{5000f, 5001f, 5002f}, 30));
    }
}
