package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.Vector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStoreTest {

    private static final String VECTOR_ID_1 = "vec1";
    private static final Vector VECTOR_1 = new Vector(VECTOR_ID_1, new double[]{1.0, 2.0, 3.0}, null);

    private static final String VECTOR_ID_2 = "vec2";
    private static final Vector VECTOR_2 = new Vector(VECTOR_ID_2, new double[]{4.0, 5.0, 6.0}, null);

    private InMemoryStore fixture;

    @BeforeEach
    void setUp() {
        fixture = new InMemoryStore();
        fixture.initFromDisk();
    }

    @AfterEach
    void tearDown() {
        fixture.dropAll();
        fixture = null;
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void insert(String id, Vector vector) {
        // Vector without metadata
        insertVector(vector);

        var retrievedVector = fixture.getVector(id);
        assertNotNull(retrievedVector);
        assertEquals(id, retrievedVector.id());
        assertArrayEquals(vector.values(), retrievedVector.values());
        assertEquals(vector.metadata(), retrievedVector.metadata());
    }

    @Test
    void search_all_vectors() {
        insertVector(VECTOR_1);
        insertVector(VECTOR_2);

        var results = fixture.search(new double[]{1.0, 2.0, 3.1}, 2);
        assertEquals(2, results.size());
        assertEquals(VECTOR_ID_1, results.get(0).id());
        assertEquals(VECTOR_ID_2, results.get(1).id());
    }

    @Test
    void search_single_nearest_vector() {
        insertVector(VECTOR_1);
        insertVector(VECTOR_2);

        var results = fixture.search(new double[]{1.0, 2.0, 3.1}, 1);
        assertEquals(1, results.size());
        assertEquals(VECTOR_ID_1, results.getFirst().id());
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void delete(String id, Vector vector) {
        insertVector(vector);

        fixture.delete(id);
        var retrievedVector = fixture.getVector(id);
        assertNull(retrievedVector);
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void getVector(String id, Vector vector) {
        insertVector(vector);

        var retrievedVector = fixture.getVector(id);
        assertNotNull(retrievedVector);
        assertEquals(id, retrievedVector.id());
        assertArrayEquals(vector.values(), retrievedVector.values());
        assertEquals(vector.metadata(), retrievedVector.metadata());
    }

    private void insertVector(Vector vector) {
        fixture.insert(vector.id(), vector);
    }

    private static Stream<Arguments> provideVectors() {
        return Stream.of(Arguments.of(VECTOR_ID_1, VECTOR_1), Arguments.of(VECTOR_ID_2, VECTOR_2));
    }
}
