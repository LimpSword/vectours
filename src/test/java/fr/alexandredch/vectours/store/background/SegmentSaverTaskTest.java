package fr.alexandredch.vectours.store.background;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.*;

import fr.alexandredch.vectours.store.base.Segment;
import fr.alexandredch.vectours.store.base.SegmentStore;
import fr.alexandredch.vectours.store.base.WriteAheadLogger;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class SegmentSaverTaskTest {

    @Mock
    private WriteAheadLogger writeAheadLogger;

    @Mock
    private SegmentStore segmentStore;

    private SegmentSaverTask fixture;

    @BeforeEach
    void setUp() {
        fixture = new SegmentSaverTask(writeAheadLogger, segmentStore);
    }

    @Test
    void saves_only_dirty_segments() {
        Segment dirtySegment = new Segment(0).setDirty(true);
        Segment cleanSegment = new Segment(1).setDirty(false);
        when(segmentStore.getSegments()).thenReturn(List.of(dirtySegment, cleanSegment));

        fixture.run();

        verify(segmentStore).saveSegmentToDisk(dirtySegment);
        verify(segmentStore, never()).saveSegmentToDisk(cleanSegment);
    }

    @Test
    void marks_segment_non_dirty_after_saving() {
        Segment dirtySegment = new Segment(0).setDirty(true);
        Segment cleanSegment = new Segment(1).setDirty(false);
        when(segmentStore.getSegments()).thenReturn(List.of(dirtySegment, cleanSegment));

        fixture.run();

        assertThat(!dirtySegment.isDirty()).isTrue();
    }

    @Test
    void updates_wal_checkpoint_after_saving_segment() {
        Segment dirtySegment = new Segment(0).setDirty(true);
        when(segmentStore.getSegments()).thenReturn(List.of(dirtySegment));

        fixture.run();

        verify(writeAheadLogger).markLastCheckpoint(dirtySegment);
    }
}
