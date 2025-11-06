package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.Metadata;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.background.SegmentSaverTask;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SegmentStore {

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    private final SegmentSaverTask segmentSaverTask;
    private final WriteAheadLogger writeAheadLogger;

    private final List<Segment> segments = new ArrayList<>();

    private Segment currentSegment;

    private boolean initialized = false;

    public SegmentStore(WriteAheadLogger writeAheadLogger) {
        this.writeAheadLogger = writeAheadLogger;

        segmentSaverTask = new SegmentSaverTask(writeAheadLogger, this);
        currentSegment = new Segment(writeAheadLogger.getLatestSegmentIdIncludingUnclosed() + 1);

        scheduledExecutorService.scheduleAtFixedRate(segmentSaverTask, 0, 30, TimeUnit.SECONDS);
    }

    public List<Vector> getAllVectors() {
        checkInitialized();
        return Stream.concat(
                        segments.stream().flatMap(segment -> segment.getVectors().stream()),
                        currentSegment.getVectors().stream())
                .collect(Collectors.toList());
    }

    public void insertVector(Vector vector) {
        checkInitialized();
        if (currentSegment.isFull()) {
            segments.add(currentSegment);

            int newSegmentId = currentSegment.getId() + 1;

            // Submit segment to background saver
            segmentSaverTask.submitSegment(currentSegment);

            // Create new segment and log it
            currentSegment = new Segment(newSegmentId);
            writeAheadLogger.newSegment(currentSegment);
        }
        currentSegment.insert(vector);
    }

    public void deleteVector(String id) {
        checkInitialized();
        for (Segment segment : segments) {
            if (segment.containsId(id)) {
                segment.delete(id);
                return;
            }
        }
        if (currentSegment.containsId(id)) {
            currentSegment.delete(id);
        }
    }

    public Vector getVectorById(String id) {
        checkInitialized();
        for (Segment segment : segments) {
            if (segment.containsId(id)) {
                return segment.getVector(id);
            }
        }
        if (currentSegment.containsId(id)) {
            return currentSegment.getVector(id);
        }
        return null;
    }

    public void saveSegmentToDisk(Segment segment) {
        Path segmentPath = Path.of("segments", "segment_" + segment.getId());
        Path vectorsPath = segmentPath.resolve("vectors");
        Path tombstonesPath = segmentPath.resolve("tombstones");

        try {
            Files.createDirectories(vectorsPath.getParent());
            Files.createDirectories(tombstonesPath.getParent());
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to create directories for segment storage", e);
        }

        try (BufferedWriter vectorWriter = Files.newBufferedWriter(vectorsPath);
             BufferedWriter tombstoneWriter = Files.newBufferedWriter(tombstonesPath)) {

            for (Vector vector : segment.getVectors()) {
                // TODO: Save as bytes
                vectorWriter.write(vector.id() + ":" + Arrays.toString(vector.values()));
                vectorWriter.newLine();
            }

            for (String tombstoneId : segment.getTombstones()) {
                tombstoneWriter.write(tombstoneId);
                tombstoneWriter.newLine();
            }

        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to save segment to disk", e);
        }
    }

    public void loadFromDisk() {
        initialized = true;

        // Load all segments from disk
        try {
            Path segmentsDir = Path.of("segments");
            if (Files.exists(segmentsDir) && Files.isDirectory(segmentsDir)) {
                Files.list(segmentsDir).filter(Files::isDirectory).forEach(segmentDir -> {
                    int segmentId =
                            Integer.parseInt(segmentDir.getFileName().toString().split("_")[1]);
                    Segment segment = new Segment(segmentId);

                    Path vectorsPath = segmentDir.resolve("vectors");
                    Path tombstonesPath = segmentDir.resolve("tombstones");

                    // Load vectors
                    if (Files.exists(vectorsPath)) {
                        try {
                            List<String> vectorLines = Files.readAllLines(vectorsPath);
                            for (String line : vectorLines) {
                                String[] parts = line.split(":");
                                String id = parts[0];
                                String valuesStr = parts[1].replaceAll("[\\[\\] ]", "");
                                double[] values = Arrays.stream(valuesStr.split(","))
                                        .mapToDouble(Double::parseDouble)
                                        .toArray();
                                Vector vector = new Vector(id, values, new Metadata(Map.of()));
                                segment.insert(vector);
                            }
                        } catch (java.io.IOException e) {
                            throw new RuntimeException("Failed to load vectors from disk", e);
                        }
                    }

                    // Load tombstones
                    if (Files.exists(tombstonesPath)) {
                        try {
                            List<String> tombstoneLines = Files.readAllLines(tombstonesPath);
                            for (String id : tombstoneLines) {
                                segment.delete(id);
                            }
                        } catch (java.io.IOException e) {
                            throw new RuntimeException("Failed to load tombstones from disk", e);
                        }
                    }

                    segments.add(segment);
                });
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to load segments from disk", e);
        }
    }

    public void close() {
        scheduledExecutorService.shutdown();

        segments.clear();
        currentSegment = null;
    }

    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("SegmentStore is not initialized. Call loadFromDisk() first.");
        }
    }
}
