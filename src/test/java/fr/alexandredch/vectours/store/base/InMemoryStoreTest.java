package fr.alexandredch.vectours.store.base;

import static org.junit.jupiter.api.Assertions.*;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.ivf.DefaultIVFIndex;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class InMemoryStoreTest {

    private static final String VECTOR_ID_1 = "vec1";
    private static final Vector VECTOR_1 = new Vector(VECTOR_ID_1, new double[] {1.0, 2.0, 3.0}, null);

    private static final String VECTOR_ID_2 = "vec2";
    private static final Vector VECTOR_2 = new Vector(VECTOR_ID_2, new double[] {4.0, 5.0, 6.0}, null);

    private static final String VECTOR_ID_3 = "vec3";
    private static final Vector VECTOR_3 = new Vector(VECTOR_ID_3, new double[] {1.0, 2.0, 3.5}, null);

    private InMemoryStore fixture;

    @BeforeEach
    void setUp() {
        fixture = new InMemoryStore();
        fixture.initFromDisk();
    }

    @AfterEach
    void tearDown() {
        fixture.dropAll();
        fixture.shutdown();
        fixture = null;
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void insert(String id, Vector vector) {
        // Vector without metadata
        insertVectors(vector);

        var retrievedVector = fixture.getVector(id);
        assertNotNull(retrievedVector);
        assertEquals(id, retrievedVector.id());
        assertArrayEquals(vector.values(), retrievedVector.values());
        assertEquals(vector.metadata(), retrievedVector.metadata());
    }

    @Test
    void search_all_vectors() {
        insertVectors(VECTOR_1, VECTOR_2);

        var results = fixture.search(new double[] {1.0, 2.0, 3.1}, 2);
        assertEquals(2, results.size());
        assertEquals(VECTOR_ID_1, results.get(0).id());
        assertEquals(VECTOR_ID_2, results.get(1).id());
    }

    @Test
    void search_single_nearest_vector() {
        insertVectors(VECTOR_1, VECTOR_2);

        var results = fixture.search(new double[] {1.0, 2.0, 3.1}, 1);
        assertEquals(1, results.size());
        assertEquals(VECTOR_ID_1, results.getFirst().id());
    }

    @Test
    void search_multiple_vectors_with_same_distance() {
        Vector vectorA = new Vector("vecA", new double[] {1.0, 1.0}, null);
        Vector vectorB = new Vector("vecB", new double[] {1.0, -1.0}, null);
        insertVectors(vectorA, vectorB);

        var results = fixture.search(new double[] {0.0, 0.0}, 2);
        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(r -> r.id().equals("vecA")));
        assertTrue(results.stream().anyMatch(r -> r.id().equals("vecB")));
    }

    @Test
    void search_with_ivf_index() {
        // There is a minimal number of vectors required to build the IVF index
        List<Vector> toInsert = new ArrayList<>();
        toInsert.add(VECTOR_1);
        for (int i = 0; i < DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX; i++) {
            toInsert.add(VECTOR_2.withId(VECTOR_ID_2 + "_" + i));
        }
        toInsert.add(VECTOR_3);
        insertVectors(toInsert.toArray(new Vector[0]));

        var results = fixture.search(VECTOR_1.values(), 2);
        assertEquals(2, results.size());
        assertEquals(VECTOR_ID_1, results.getFirst().id());
        assertEquals(VECTOR_ID_3, results.get(1).id());
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void delete(String id, Vector vector) throws ExecutionException, InterruptedException {
        insertVectors(vector);

        fixture.delete(id).get();
        var retrievedVector = fixture.getVector(id);
        assertNull(retrievedVector);
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void getVector(String id, Vector vector) {
        insertVectors(vector);

        var retrievedVector = fixture.getVector(id);
        assertNotNull(retrievedVector);
        assertEquals(id, retrievedVector.id());
        assertArrayEquals(vector.values(), retrievedVector.values());
        assertEquals(vector.metadata(), retrievedVector.metadata());
    }

    private void insertVectors(Vector... vectors) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Vector vector : vectors) {
            futures.add(fixture.insert(vector));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private static Stream<Arguments> provideVectors() {
        return Stream.of(Arguments.of(VECTOR_ID_1, VECTOR_1), Arguments.of(VECTOR_ID_2, VECTOR_2));
    }
}
