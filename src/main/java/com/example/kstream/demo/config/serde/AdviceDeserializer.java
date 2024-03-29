package com.example.kstream.demo.config.serde;

import com.example.kstream.demo.model.Generator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AdviceDeserializer implements Deserializer<Generator> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuración adicional si es necesaria
    }

    @Override
    public Generator deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            // Transformar el array de bytes a un objeto Advice (en este ejemplo se utiliza una representación en JSON)
            String jsonData = new String(data, StandardCharsets.UTF_8);
            return convertJsonToAdvice(jsonData);
        } catch (Exception e) {
            throw new RuntimeException("Error al deserializar Advice", e);
        }
    }

    private Generator convertJsonToAdvice(String jsonData) {
        try {
            return objectMapper.readValue(jsonData, Generator.class);
        } catch (Exception e) {
            throw new RuntimeException("Error al deserializar Insights", e);
        }
    }

    @Override
    public void close() {
        // Cierre de recursos si es necesario
    }
}