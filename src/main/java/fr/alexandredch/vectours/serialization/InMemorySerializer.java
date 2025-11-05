package fr.alexandredch.vectours.serialization;

import fr.alexandredch.vectours.data.Vector;
import java.io.*;
import java.util.Map;

public final class InMemorySerializer {

    public static Map<String, Vector> deserialize(byte[] data) throws IOException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
        try {
            return (Map<String, Vector>) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(Map<String, Vector> map) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(map);
        objectOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }
}
