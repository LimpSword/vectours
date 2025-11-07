package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.operations.Operation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@ExtendWith(MockitoExtension.class)
public class WriteAheadLoggerTest {

    private static final Path LOG_FILE_PATH = Paths.get(WriteAheadLogger.LOG_FILE_NAME);
    private static final Segment OLD_SEGMENT = new Segment(1);
    private static final Segment SEGMENT = new Segment(4);

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
        fixture.markLastCheckpoint(OLD_SEGMENT);

        fixture.newSegment(SEGMENT);
        Operation.Delete op1 = new Operation.Delete("1");
        Operation.Delete op2 = new Operation.Delete("2");
        fixture.applyOperation(op1);
        fixture.applyOperation(op2);

        var operations = fixture.loadFromCheckpoint();
        assertThat(operations.size()).isEqualTo(2);
        assertThat(operations.get(0)).usingRecursiveComparison().isEqualTo(op1);
        assertThat(operations.get(1)).usingRecursiveComparison().isEqualTo(op2);
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
        Operation.Delete operation = new Operation.Delete("42");
        fixture.applyOperation(operation);
        // Read the log file and verify the operation is present
        String logContent = Files.readString(LOG_FILE_PATH);
        byte[] operationBytes = operation.toBytes();
        for (byte b : operationBytes) {
            assertThat(logContent.getBytes()).contains(b);
        }
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
        assertThat(checkpointContent.trim()).isEqualTo(Integer.toString(SEGMENT.getId()));
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
