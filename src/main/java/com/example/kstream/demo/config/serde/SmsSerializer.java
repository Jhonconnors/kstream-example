package com.example.kstream.demo.config.serde;

import com.example.kstream.demo.model.SmsNotification;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SmsSerializer implements Serializer<SmsNotification> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Configuración adicional si es necesaria
    }

    @Override
    public byte[] serialize(String topic, SmsNotification data) {
        if (data == null) {
            return null;
        }
        try {
            // Transformar el objeto Advice a un array de bytes (en este ejemplo se utiliza una representación en JSON)
            String jsonData = convertSmsToJson(data);
            return jsonData.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Error al serializar Advice", e);
        }
    }

    private String convertSmsToJson(SmsNotification sms) {
        // Lógica para convertir un objeto Advice a una representación en JSON
        // Implementa la lógica adecuada según tus requisitos y bibliotecas de serialización (por ejemplo, Gson, Jackson, etc.)
        // Ejemplo básico:
        return "{\"id_notification\":" + sms.getIdNotification() + ",\"destination_number\":\"" + sms.getDestinationNumber()
                + "\",\"notification_date\":\"" + sms.getNotificationDate() + "\",\"message_body\":" + sms.getMessageBody() + "}";
    }

    @Override
    public void close() {
        // Cierre de recursos si es necesario
    }
}