package com.example.kstream.demo.config.serde;

import com.example.kstream.demo.model.ClientInsight;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class InsightsDeserializer implements Deserializer<ClientInsight> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuración adicional si es necesaria
    }

    @Override
    public ClientInsight deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            // Transformar el array de bytes a un objeto Insights (en este ejemplo se utiliza una representación en JSON)
            String jsonData = new String(data, StandardCharsets.UTF_8);
            return convertJsonToInsights(jsonData);
        } catch (Exception e) {
            throw new RuntimeException("Error al deserializar Insights", e);
        }
    }

    private ClientInsight convertJsonToInsights(String jsonData) {
        try {
            return objectMapper.readValue(jsonData, ClientInsight.class);
        } catch (Exception e) {
            throw new RuntimeException("Error al deserializar Insights", e);
        }
    }

    @Override
    public void close() {
        // Cierre de recursos si es necesario
    }
}