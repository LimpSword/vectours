package fr.alexandredch.vectours.store.segment.vector;

import com.google.common.primitives.Bytes;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.segment.Segment;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public final class SegmentVectorStore {

    public static final byte SEPARATOR_BYTES = '\u001D';

    private final VectorSerializer serializer = new VectorSerializer();

    public void writeSegmentVectorsToDisk(Path segmentPath, Segment segment) {
        Path vectorsPath = segmentPath.resolve(SegmentStore.VECTORS_FILE);

        try {
            Files.createDirectories(vectorsPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directories for segment storage", e);
        }

        try (OutputStream outputStream = Files.newOutputStream(vectorsPath);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream)) {

            for (Vector vector : segment.getVectors()) {
                byte[] serializedVector = serializer.encodeVector(vector);
                bufferedOutputStream.write(Bytes.concat(serializedVector, new byte[] {SEPARATOR_BYTES}));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write segment to disk", e);
        }
    }

    public Vector[] readSegmentVectorsFromDisk(Path segmentPath) {
        Path vectorsPath = segmentPath.resolve(SegmentStore.VECTORS_FILE);

        if (!Files.exists(vectorsPath)) {
            throw new RuntimeException("Segment file does not exist: " + vectorsPath);
        }

        Vector[] vectors = new Vector[0];

        try (InputStream inputStream = Files.newInputStream(vectorsPath);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

            int b;
            while ((b = bufferedInputStream.read()) != -1) {
                if (b == SEPARATOR_BYTES) {
                    byte[] vectorBytes = byteArrayOutputStream.toByteArray();
                    if (vectorBytes.length > 0) {
                        Vector vector = serializer.decodeVector(vectorBytes);
                        vectors = appendVector(vectors, vector);
                        byteArrayOutputStream.reset();
                    }
                } else {
                    byteArrayOutputStream.write(b);
                }
            }

            // Handle last vector if file does not end with a newline
            if (byteArrayOutputStream.size() > 0) {
                Vector vector = serializer.decodeVector(byteArrayOutputStream.toByteArray());
                vectors = appendVector(vectors, vector);
            }

            return vectors;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment from disk", e);
        }
    }

    private Vector[] appendVector(Vector[] vectors, Vector vector) {
        Vector[] newVectors = new Vector[vectors.length + 1];
        System.arraycopy(vectors, 0, newVectors, 0, vectors.length);
        newVectors[vectors.length] = vector;
        return newVectors;
    }
}
