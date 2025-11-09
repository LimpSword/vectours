package fr.alexandredch.vectours.operations;

import fr.alexandredch.vectours.data.Vector;
import java.io.Serializable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;

public sealed interface Operation extends Serializable permits Operation.Delete, Operation.Insert {

    record Insert(Vector vector) implements Operation {}

    record Delete(String id) implements Operation {}

    static byte[] toBytes(Operation operation) {
        return ArrayUtils.addAll(SerializationUtils.serialize(operation));
    }

    static Operation fromBytes(byte[] bytes) {
        return SerializationUtils.deserialize(bytes);
    }
}
