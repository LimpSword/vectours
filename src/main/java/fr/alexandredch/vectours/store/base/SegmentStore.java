package fr.alexandredch.vectours.store.base;

import fr.alexandredch.vectours.data.Metadata;
import fr.alexandredch.vectours.data.Vector;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SegmentStore {

    public static final String SEGMENTS_DIR = "segments";
    public static final String SEGMENT_FILE_PREFIX = "segment_";
    public static final String VECTORS_FILE = "vectors";
    public static final String TOMBSTONES_FILE = "tombstones";

    private final WriteAheadLogger writeAheadLogger;

    private final List<Segment> segments = new ArrayList<>();

    private Segment currentSegment;

    private boolean initialized = false;

    public SegmentStore(WriteAheadLogger writeAheadLogger) {
        this.writeAheadLogger = writeAheadLogger;

        currentSegment = new Segment(writeAheadLogger.getLatestSegmentIdIncludingUnclosed() + 1);
    }

    public List<Segment> getSegments() {
        return Stream.concat(segments.stream(), Stream.of(currentSegment)).collect(Collectors.toList());
    }

    public int getTotalVectorCount() {
        checkInitialized();
        return segments.stream().mapToInt(Segment::size).sum() + currentSegment.size();
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
        Path segmentPath = Path.of(SEGMENTS_DIR, SEGMENT_FILE_PREFIX + segment.getId());
        Path vectorsPath = segmentPath.resolve(VECTORS_FILE);
        Path tombstonesPath = segmentPath.resolve(TOMBSTONES_FILE);

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
                vectorWriter.write(vector.toString());
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
        // Load all segments from disk
        try {
            Path segmentsDir = Path.of(SEGMENTS_DIR);
            if (Files.exists(segmentsDir) && Files.isDirectory(segmentsDir)) {
                Files.list(segmentsDir).filter(Files::isDirectory).forEach(segmentDir -> {
                    int segmentId =
                            Integer.parseInt(segmentDir.getFileName().toString().split("_")[1]);
                    Segment segment = new Segment(segmentId);

                    Path vectorsPath = segmentDir.resolve(VECTORS_FILE);
                    Path tombstonesPath = segmentDir.resolve(TOMBSTONES_FILE);

                    // Load vectors
                    if (Files.exists(vectorsPath)) {
                        String lastLine = "No lines read";
                        try {
                            List<String> vectorLines = Files.readAllLines(vectorsPath);
                            for (String line : vectorLines) {
                                lastLine = line;
                                String[] parts = line.split(":");
                                String id = parts[0];
                                String valuesStr = parts[1].replaceAll("[\\[\\] ]", "");
                                double[] values = Arrays.stream(valuesStr.split(","))
                                        .mapToDouble(Double::parseDouble)
                                        .toArray();
                                Vector vector = new Vector(id, values, new Metadata(Map.of()));
                                segment.insert(vector);
                            }
                        } catch (IOException e) {
                            System.out.println(lastLine);
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

        initialized = true;
    }

    public void close() {
        segments.clear();
        currentSegment = null;
        initialized = false;

        Path segmentsDir = Path.of(SEGMENTS_DIR);
        if (!Files.exists(segmentsDir)) {
            return;
        }
        try (var dirStream = Files.walk(segmentsDir)) {
            dirStream.map(Path::toFile).sorted(Comparator.reverseOrder()).forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete segments directory", e);
        }
    }

    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("SegmentStore is not initialized. Call loadFromDisk() first.");
        }
    }
}
