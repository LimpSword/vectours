package fr.alexandredch.vectours.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.index.ivf.DefaultIVFIndex;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class IVFIndexTest {

    @Mock
    private SegmentStore segmentStore;

    private DefaultIVFIndex fixture;

    @BeforeEach
    public void setUp() {
        fixture = new DefaultIVFIndex(segmentStore);
    }

    @Test
    void insertVector_adds_to_existing_cluster_if_index_is_built() {
        when(segmentStore.getTotalVectorCount()).thenReturn(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1);
        when(segmentStore.getAllVectors()).thenReturn(getVectors(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1));

        Vector triggerVector = new Vector(
                "dummy" + (DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1),
                new double[] {DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1},
                null);
        // This will trigger the index building
        fixture.insertVector(triggerVector);

        int vectorsInCluster = fixture.getClusters().stream()
                .map(vectorCluster -> vectorCluster.getData().size())
                .reduce(0, Integer::sum);

        Vector lastVector = new Vector(
                "dummy" + (DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 2),
                new double[] {DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1},
                null);
        // Index is built,
        fixture.insertVector(lastVector);

        int vectorsInNewCluster = fixture.getClusters().stream()
                .map(vectorCluster -> vectorCluster.getData().size())
                .reduce(0, Integer::sum);
        assertThat(vectorsInNewCluster).isEqualTo(vectorsInCluster + 1);
    }

    @Test
    void insertVector_does_nothing_if_index_is_not_built_and_not_enough_vectors() {
        when(segmentStore.getTotalVectorCount()).thenReturn(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX - 1);

        Vector triggerVector = new Vector(
                "dummy" + (DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1),
                new double[] {DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1},
                null);
        // This will NOT trigger the index building
        fixture.insertVector(triggerVector);

        assertThat(fixture.getClusters()).isEmpty();
    }

    @Test
    void insertVector_creates_index_if_not_built_and_enough_vectors() {
        when(segmentStore.getTotalVectorCount()).thenReturn(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1);
        when(segmentStore.getAllVectors()).thenReturn(getVectors(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1));

        Vector triggerVector = new Vector(
                "dummy" + (DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1),
                new double[] {DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1},
                null);
        // This will NOT trigger the index building
        fixture.insertVector(triggerVector);

        assertThat(fixture.getClusters()).isNotEmpty();
    }

    @Test
    void canSearch_returns_false_if_index_is_not_built() {
        assertThat(fixture.canSearch()).isFalse();
    }

    @Test
    void canSearch_returns_true_if_index_is_built() {
        when(segmentStore.getTotalVectorCount()).thenReturn(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1);
        when(segmentStore.getAllVectors()).thenReturn(getVectors(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1));
        fixture.insertVector(new Vector("dummy", new double[] {1}, null));

        assertThat(fixture.canSearch()).isTrue();
    }

    @Test
    void search_returns_closest_vectors() {
        when(segmentStore.getTotalVectorCount()).thenReturn(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1);
        when(segmentStore.getAllVectors()).thenReturn(getVectors(DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1));
        fixture.insertVector(new Vector("dummy", new double[] {DefaultIVFIndex.MIN_VECTORS_FOR_IVF_INDEX + 1}, null));

        List<Vector> results = fixture.search(new double[] {1}, 2);

        assertThat(results).hasSize(2);
        assertThat(results.get(0).id()).isEqualTo("dummy1");
        assertThat(results.get(1).id()).isEqualTo("dummy0");
    }

    @Test
    void search_returns_empty_list_if_no_index() {
        List<Vector> results = fixture.search(new double[] {1}, 2);

        assertThat(results).isEmpty();
    }

    private List<Vector> getVectors(int count) {
        List<Vector> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            vectors.add(new Vector("dummy" + i, new double[] {i}, null));
        }
        return vectors;
    }
}
