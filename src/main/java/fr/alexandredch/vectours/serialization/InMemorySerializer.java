package fr.alexandredch.vectours.serialization;

import java.io.*;

public final class InMemorySerializer {

    public static double[] deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return (double[]) objectInputStream.readObject();
    }

    public static byte[] serialize(double[] vector) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(vector);
        objectOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }
}
