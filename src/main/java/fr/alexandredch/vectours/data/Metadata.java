package fr.alexandredch.vectours.data;

import java.io.Serializable;
import java.util.Map;

public record Metadata(Map<String, Object> metadata) implements Serializable {}
