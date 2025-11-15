package fr.alexandredch.vectours.store.base;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.alexandredch.vectours.data.Metadata;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.operations.Operation;
import fr.alexandredch.vectours.store.segment.Segment;
import fr.alexandredch.vectours.store.wal.WriteAheadLogger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WriteAheadLoggerTest {

    private static final Path LOG_FILE_PATH = Paths.get(WriteAheadLogger.LOG_FILE_NAME);
    private static final Segment OLD_SEGMENT = new Segment(1);
    private static final Segment SEGMENT = new Segment(4);

    private static final Metadata METADATA = new Metadata(new ObjectMapper().createObjectNode());

    private static final String VECTOR_ID_1 = "vec1";
    private static final Vector VECTOR_WITH_METADATA = new Vector(VECTOR_ID_1, new double[] {1.0, 2.0, 3.0}, METADATA);

    private WriteAheadLogger fixture;

    @BeforeEach
    void setUp() {
        fixture = new WriteAheadLogger();
        fixture.clearLog();
    }

    @AfterEach
    void tearDown() {
        fixture.clearLog();
    }

    @Test
    void loadFromCheckpoint_returns_empty_list_when_no_log_file() {
        var operations = fixture.loadFromCheckpoint();
        assertThat(operations.size()).isEqualTo(0);
    }

    @Test
    void loadFromCheckpoint_returns_operations_after_checkpointed_segment() throws Exception {
        // Prepare the log file with a segment and some operations
        fixture.newSegment(OLD_SEGMENT);
        Operation.Delete op0 = new Operation.Delete("0");
        fixture.applyOperation(op0).get();
        fixture.markLastCheckpoint(OLD_SEGMENT);

        fixture.newSegment(SEGMENT);
        Operation.Delete op1 = new Operation.Delete("1");
        Operation.Delete op2 = new Operation.Delete("2");
        fixture.applyOperation(op1).get();
        fixture.applyOperation(op2).get();

        var operations = fixture.loadFromCheckpoint();
        assertThat(operations.size()).isEqualTo(3);
        assertThat(operations.get(0))
                .usingRecursiveComparison()
                .isEqualTo(new Operation.CreateSegment(SEGMENT.getId()));
        assertThat(operations.get(1)).usingRecursiveComparison().isEqualTo(op1);
        assertThat(operations.get(2)).usingRecursiveComparison().isEqualTo(op2);
    }

    @Test
    void loadFromCheckpoint_returns_operations_even_if_no_checkpoint_file() throws Exception {
        // Prepare the log file with a segment and some operations
        fixture.newSegment(SEGMENT);
        Operation.Delete op1 = new Operation.Delete("1");
        Operation.Delete op2 = new Operation.Delete("2");
        fixture.applyOperation(op1).get();
        fixture.applyOperation(op2).get();

        var operations = fixture.loadFromCheckpoint();
        assertThat(operations.size()).isEqualTo(3);
        assertThat(operations.get(0))
                .usingRecursiveComparison()
                .isEqualTo(new Operation.CreateSegment(SEGMENT.getId()));
        assertThat(operations.get(1)).usingRecursiveComparison().isEqualTo(op1);
        assertThat(operations.get(2)).usingRecursiveComparison().isEqualTo(op2);
    }

    @Test
    void newSegment_appends_segment_id_to_log() throws Exception {
        fixture.newSegment(SEGMENT);
        // Read the log file and verify the segment id is present
        String logContent = Files.readString(LOG_FILE_PATH);
        assertThat(logContent).contains(Integer.toString(SEGMENT.getId()));
    }

    @Test
    void applyOperation_appends_operation_to_log() throws Exception {
        Operation.Delete operation1 = new Operation.Delete("42");
        fixture.applyOperation(operation1).get();
        Operation.Delete operation2 = new Operation.Delete("43");
        fixture.applyOperation(operation2).get();
        // Read the log file and verify the operations are present
        byte[] logBytes = Files.readAllBytes(LOG_FILE_PATH);
        byte[] expectedBytes = Stream.of(operation1, operation2)
                .flatMap(op -> Stream.of(Operation.toBytes(op), WriteAheadLogger.SEPARATOR_BYTES))
                .reduce(new byte[0], ArrayUtils::addAll);
        assertThat(new String(logBytes)).isEqualTo(new String(expectedBytes));
    }

    @Test
    void applyOperations_accepts_metadata() throws ExecutionException, InterruptedException {
        Operation.Insert operation1 = new Operation.Insert(VECTOR_WITH_METADATA);
        fixture.applyOperation(operation1).get();

        var operations = fixture.loadFromCheckpoint();
        assertThat(operations.size()).isEqualTo(1);
        Operation operation = operations.getFirst();
        assertThat(operation).isInstanceOf(Operation.InsertInSegment.class);
        assertThat(((Operation.InsertInSegment) operation).vector())
                .usingRecursiveComparison()
                .isEqualTo(VECTOR_WITH_METADATA);
    }

    @Test
    void getLastCheckpointedSegmentId_returns_minus_one_when_no_checkpoint_file() {
        int lastCheckpointedSegmentId = fixture.getLastCheckpointedSegmentId();
        assertThat(lastCheckpointedSegmentId).isEqualTo(-1);
    }

    @Test
    void markLastCheckpoint_updates_checkpoint_file() throws Exception {
        fixture.markLastCheckpoint(SEGMENT);
        // Read the checkpoint file and verify it contains the segment id
        String checkpointContent = Files.readString(Paths.get(WriteAheadLogger.CHECKPOINT_FILE_NAME));
        assertThat(checkpointContent).isEqualTo(Integer.toString(SEGMENT.getId()));
    }

    @Test
    void getLastCheckpointedSegmentId_returns_correct_segment_id() throws Exception {
        fixture.markLastCheckpoint(SEGMENT);
        int lastCheckpointedSegmentId = fixture.getLastCheckpointedSegmentId();
        assertThat(lastCheckpointedSegmentId).isEqualTo(SEGMENT.getId());
    }

    @Test
    void clearLog_deletes_log_file() {
        // Ensure the log file exists
        fixture.newSegment(SEGMENT);
        assertThat(Files.exists(LOG_FILE_PATH)).isTrue();

        fixture.clearLog();
        // The log file should be deleted
        assertThat(Files.exists(LOG_FILE_PATH)).isFalse();
    }
}
