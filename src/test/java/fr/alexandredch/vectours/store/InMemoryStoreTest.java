package fr.alexandredch.vectours.store;

import fr.alexandredch.vectours.data.Vector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStoreTest {

    private static final String VECTOR_ID_1 = "vec1";
    private static final Vector VECTOR_1 = new Vector(VECTOR_ID_1, new float[]{1.0f, 2.0f, 3.0f}, null);

    private static final String VECTOR_ID_2 = "vec2";
    private static final Vector VECTOR_2 = new Vector(VECTOR_ID_2, new float[]{4.0f, 5.0f, 6.0f}, null);

    private final InMemoryStore fixture = new InMemoryStore();

    @AfterEach
    void tearDown() {
        fixture.dropAll();
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void insert(String id, Vector vector) {
        // Vector without metadata
        insertVector(vector);

        var retrievedVector = fixture.getVector(id);
        assertNotNull(retrievedVector);
        assertEquals(id, retrievedVector.getId());
        assertArrayEquals(vector.getValues(), retrievedVector.getValues());
        assertEquals(vector.getMetadata(), retrievedVector.getMetadata());
    }

    @Test
    void search_all_vectors() {
        insertVector(VECTOR_1);
        insertVector(VECTOR_2);

        var results = fixture.search(new float[]{1.0f, 2.0f, 3.1f}, 2);
        assertEquals(2, results.size());
        assertEquals(VECTOR_ID_1, results.get(0).id());
        assertEquals(VECTOR_ID_2, results.get(1).id());
    }

    @Test
    void search_single_nearest_vector() {
        insertVector(VECTOR_1);
        insertVector(VECTOR_2);

        var results = fixture.search(new float[]{1.0f, 2.0f, 3.1f}, 1);
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
        assertEquals(id, retrievedVector.getId());
        assertArrayEquals(vector.getValues(), retrievedVector.getValues());
        assertEquals(vector.getMetadata(), retrievedVector.getMetadata());
    }

    @Test
    void initAndSave() {
        insertVector(VECTOR_1);
        insertVector(VECTOR_2);

        fixture.save();

        InMemoryStore newStore = new InMemoryStore();
        newStore.initFromDisk();

        var retrievedVector1 = newStore.getVector(VECTOR_ID_1);
        assertNotNull(retrievedVector1);
        assertEquals(VECTOR_ID_1, retrievedVector1.getId());
        assertArrayEquals(VECTOR_1.getValues(), retrievedVector1.getValues());
        assertEquals(VECTOR_1.getMetadata(), retrievedVector1.getMetadata());

        var retrievedVector2 = newStore.getVector(VECTOR_ID_2);
        assertNotNull(retrievedVector2);
        assertEquals(VECTOR_ID_2, retrievedVector2.getId());
        assertArrayEquals(VECTOR_2.getValues(), retrievedVector2.getValues());
        assertEquals(VECTOR_2.getMetadata(), retrievedVector2.getMetadata());
    }

    private void insertVector(Vector vector) {
        fixture.insert(vector.getId(), vector);
    }

    private static Stream<Arguments> provideVectors() {
        return Stream.of(
                Arguments.of(VECTOR_ID_1, VECTOR_1),
                Arguments.of(VECTOR_ID_2, VECTOR_2)
        );
    }
}