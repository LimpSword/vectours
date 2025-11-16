package fr.alexandredch.vectours.store.segment.tombstone;

import fr.alexandredch.vectours.store.segment.Segment;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class SegmentTombstoneStore {

    public void writeSegmentTombstonesToDisk(Path segmentPath, Segment segment) {
        Path tombstonesPath = segmentPath.resolve(SegmentStore.TOMBSTONES_FILE);

        try {
            Files.createDirectories(tombstonesPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directories for segment storage", e);
        }

        try (BufferedWriter tombstoneWriter = Files.newBufferedWriter(tombstonesPath)) {
            for (String tombstoneId : segment.getTombstones()) {
                tombstoneWriter.write(tombstoneId);
                tombstoneWriter.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save segment to disk", e);
        }
    }

    public List<String> readSegmentTombstonesFromDisk(Path segmentPath) {
        Path tombstonesPath = segmentPath.resolve(SegmentStore.TOMBSTONES_FILE);

        if (!Files.exists(tombstonesPath)) {
            throw new RuntimeException("Segment file does not exist: " + tombstonesPath);
        }

        try {
            return Files.readAllLines(tombstonesPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment from disk", e);
        }
    }
}
