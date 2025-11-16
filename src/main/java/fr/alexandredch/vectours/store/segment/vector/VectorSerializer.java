package fr.alexandredch.vectours.store.segment.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import fr.alexandredch.vectours.data.Metadata;
import fr.alexandredch.vectours.data.Vector;
import java.io.*;

public final class VectorSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

    public byte[] encodeVector(Vector vector) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeUTF(vector.id());

        double[] values = vector.values();
        out.writeInt(values.length);

        for (double v : values) {
            out.writeDouble(v);
        }

        byte[] metadata = objectMapper.writeValueAsBytes(vector.metadata());
        out.write(metadata);

        out.flush();
        out.close();

        return baos.toByteArray();
    }

    public Vector decodeVector(byte[] vector) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(vector));

        String id = in.readUTF();

        int len = in.readInt();
        double[] values = new double[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readDouble();
        }

        byte[] metadataBytes = in.readAllBytes();
        Metadata m = objectMapper.readValue(metadataBytes, Metadata.class);

        return new Vector(id, values, m);
    }
}
