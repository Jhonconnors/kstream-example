package com.example.kstream.demo.config.serde;

import com.example.kstream.demo.model.Insights;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class InsightsSerializer implements Serializer<Insights> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuración adicional si es necesaria
    }

    @Override
    public byte[] serialize(String topic, Insights data) {
        if (data == null) {
            return null;
        }
        try {
            // Transformar el objeto Insights a un array de bytes (en este ejemplo se utiliza una representación en JSON)
            String jsonData = convertInsightsToJson(data);
            return jsonData.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Error al serializar Insights", e);
        }
    }

    private String convertInsightsToJson(Insights insights) {
        // Lógica para convertir un objeto Insights a una representación en JSON
        // Implementa la lógica adecuada según tus requisitos y bibliotecas de serialización (por ejemplo, Gson, Jackson, etc.)
        // Ejemplo básico:
        return "{\"id\":" + insights.getId() + ",\"variante\":\"" + insights.getVariante() + "\",\"timeStamp\":\"" + insights.getTimeStamp() + "\",\"variables\":" + insights.getVariables() + "}";
    }

    @Override
    public void close() {
        // Cierre de recursos si es necesario
    }
}