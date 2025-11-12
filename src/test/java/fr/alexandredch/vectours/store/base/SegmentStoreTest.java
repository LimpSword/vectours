package fr.alexandredch.vectours.store.base;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.wal.WriteAheadLogger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class SegmentStoreTest {

    private static final String VECTOR_ID_1 = "vec1";
    private static final Vector VECTOR_1 = new Vector(VECTOR_ID_1, new double[] {1.0, 2.0, 3.0}, null);

    private static final String VECTOR_ID_2 = "vec2";
    private static final Vector VECTOR_2 = new Vector(VECTOR_ID_2, new double[] {4.0, 5.0, 6.0}, null);

    @Mock
    private WriteAheadLogger writeAheadLogger;

    private SegmentStore fixture;

    @BeforeEach
    public void setUp() {
        fixture = new SegmentStore(writeAheadLogger);
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Clean up segments directory after each test
        Path segmentsDir = Path.of(SegmentStore.SEGMENTS_DIR);
        if (Files.exists(segmentsDir)) {
            Files.walk(segmentsDir)
                    .sorted(Comparator.reverseOrder()) // Delete children before parents
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    @Test
    void insertVector_requiresInitialization() {
        assertThrows(IllegalStateException.class, () -> fixture.insertVector(VECTOR_1));
    }

    @Test
    void insertVector_addsVectorToCurrentSegment() {
        fixture.loadFromDisk();

        assertThat(fixture.getAllVectors()).isEmpty();

        fixture.insertVector(VECTOR_1);

        assertThat(fixture.getAllVectors()).containsExactly(VECTOR_1);
    }

    @Test
    void insertVector_createsNewSegmentWhenCurrentIsFull() {
        fixture.loadFromDisk();

        for (int i = 0; i < Segment.MAX_SEGMENT_SIZE; i++) {
            fixture.insertVector(new Vector("dummy" + i, new double[] {i}, null));
        }

        fixture.insertVector(VECTOR_1);

        assertThat(fixture.getSegments()).hasSize(2);
        assertThat(fixture.getAllVectors()).contains(VECTOR_1);
    }

    @Test
    void deleteVector_requiresInitialization() {
        assertThrows(IllegalStateException.class, () -> fixture.deleteVector(VECTOR_ID_1));
    }

    @Test
    void deleteVector_removesVectorFromCurrentSegment() {
        fixture.loadFromDisk();

        fixture.insertVector(VECTOR_1);
        fixture.insertVector(VECTOR_2);

        fixture.deleteVector(VECTOR_ID_1);

        assertThat(fixture.getAllVectors()).doesNotContain(VECTOR_1);
        assertThat(fixture.getAllVectors()).contains(VECTOR_2);
    }

    @Test
    void deleteVector_removesVectorFromExistingSegment() {
        fixture.loadFromDisk();

        for (int i = 0; i < Segment.MAX_SEGMENT_SIZE; i++) {
            fixture.insertVector(new Vector("dummy" + i, new double[] {i}, null));
        }
        fixture.insertVector(VECTOR_1);
        fixture.insertVector(VECTOR_2);

        fixture.deleteVector(VECTOR_ID_1);

        assertThat(fixture.getAllVectors()).doesNotContain(VECTOR_1);
        assertThat(fixture.getAllVectors()).contains(VECTOR_2);
    }

    @Test
    void getVectorById_requiresInitialization() {
        assertThrows(IllegalStateException.class, () -> fixture.getVectorById(VECTOR_ID_1));
    }

    @Test
    void getVectorById_returnsVectorFromCurrentSegment() {
        fixture.loadFromDisk();

        fixture.insertVector(VECTOR_1);

        Vector retrieved = fixture.getVectorById(VECTOR_ID_1);

        assertThat(retrieved).isEqualTo(VECTOR_1);
    }

    @Test
    void getVectorById_returnsVectorFromExistingSegment() {
        fixture.loadFromDisk();

        for (int i = 0; i < Segment.MAX_SEGMENT_SIZE; i++) {
            fixture.insertVector(new Vector("dummy" + i, new double[] {i}, null));
        }
        fixture.insertVector(VECTOR_1);

        Vector retrieved = fixture.getVectorById(VECTOR_ID_1);

        assertThat(retrieved).isEqualTo(VECTOR_1);
    }

    @Test
    void getVectorById_returnsNullIfNotFound() {
        fixture.loadFromDisk();

        Vector retrieved = fixture.getVectorById("nonexistent");

        assertThat(retrieved).isNull();
    }

    @Test
    void loadFromDisk_initializesStore() {
        fixture.loadFromDisk();

        assertThat(fixture.getAllVectors()).isEmpty();
        assertThat(fixture.getSegments()).hasSize(1); // Current segment only
    }

    @Test
    void saveSegmentToDisk_createsCorrectFiles() {
        fixture.loadFromDisk();

        Segment segment = new Segment(0);
        segment.insert(VECTOR_1);
        segment.insert(VECTOR_2);

        segment.delete(VECTOR_ID_2);

        fixture.saveSegmentToDisk(segment);

        // There should be a folder named SegmentStore.SEGMENTS_DIR/SegmentStore.SEGMENT_FILE_PREFIX + "0"
        // containing two files: SegmentStore.VECTORS_FILE and SegmentStore.TOMBSTONES_FILE
        Path segmentPath = Path.of(SegmentStore.SEGMENTS_DIR, SegmentStore.SEGMENT_FILE_PREFIX + segment.getId());
        Path vectorsPath = segmentPath.resolve(SegmentStore.VECTORS_FILE);
        Path tombstonesPath = segmentPath.resolve(SegmentStore.TOMBSTONES_FILE);

        assertThat(vectorsPath).exists();
        assertThat(tombstonesPath).exists();

        // Vectors file should contain VECTOR_1 but not VECTOR_2 (because it was deleted)
        try {
            String vectorsContent = Files.readString(vectorsPath);
            assertThat(vectorsContent).contains(VECTOR_ID_1);
            assertThat(vectorsContent).doesNotContain(VECTOR_ID_2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Tombstones file should contain VECTOR_ID_2
        try {
            String tombstonesContent = Files.readString(tombstonesPath);
            assertThat(tombstonesContent).contains(VECTOR_ID_2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
