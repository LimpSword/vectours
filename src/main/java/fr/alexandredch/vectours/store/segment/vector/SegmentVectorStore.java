package fr.alexandredch.vectours.store.segment.vector;

import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.segment.Segment;
import fr.alexandredch.vectours.store.segment.SegmentStore;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public final class SegmentVectorStore {

    private final VectorSerializer serializer = new VectorSerializer();

    public void writeSegmentVectorsToDisk(Path segmentPath, Segment segment) {
        Path vectorsPath = segmentPath.resolve(SegmentStore.VECTORS_FILE);

        try {
            Files.createDirectories(vectorsPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directories for segment storage", e);
        }

        try (OutputStream outputStream = Files.newOutputStream(vectorsPath);
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream)) {

            for (Vector vector : segment.getVectors()) {
                byte[] serializedVector = serializer.encodeVector(vector);
                // Write length first, then the serialized vector
                dataOutputStream.writeInt(serializedVector.length);
                dataOutputStream.write(serializedVector);
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
                DataInputStream dataInputStream = new DataInputStream(bufferedInputStream)) {

            while (dataInputStream.available() > 0) {
                // Read length first, then the serialized vector
                int length = dataInputStream.readInt();
                byte[] vectorBytes = new byte[length];
                dataInputStream.readFully(vectorBytes);

                Vector vector = serializer.decodeVector(vectorBytes);
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
