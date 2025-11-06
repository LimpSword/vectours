package fr.alexandredch.vectours.operations;

import java.nio.ByteBuffer;

public sealed interface Operation permits Operation.Insert, Operation.Delete {

    byte[] toBytes();

    record Insert(String id, byte[] vectorBytes) implements Operation {
        @Override
        public byte[] toBytes() {
            return ByteBuffer.allocate(4 + id.length() + 4 + vectorBytes.length)
                    .putInt(id.length())
                    .put(id.getBytes())
                    .putInt(vectorBytes.length)
                    .put(vectorBytes)
                    .array();
        }
    }

    record Delete(String id) implements Operation {
        @Override
        public byte[] toBytes() {
            return ByteBuffer.allocate(4 + id.length())
                    .putInt(id.length())
                    .put(id.getBytes())
                    .array();
        }
    }

    static Operation fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int idLength = buffer.getInt();
        byte[] idBytes = new byte[idLength];
        buffer.get(idBytes);
        String id = new String(idBytes);
        if (buffer.hasRemaining()) {
            int vectorLength = buffer.getInt();
            byte[] vectorBytes = new byte[vectorLength];
            buffer.get(vectorBytes);
            return new Insert(id, vectorBytes);
        } else {
            return new Delete(id);
        }
    }
}
