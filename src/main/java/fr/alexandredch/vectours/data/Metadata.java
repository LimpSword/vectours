package fr.alexandredch.vectours.data;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;

public record Metadata(JsonNode metadata) implements Serializable {}
