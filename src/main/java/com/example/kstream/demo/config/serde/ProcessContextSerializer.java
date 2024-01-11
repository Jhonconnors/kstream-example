package com.example.kstream.demo.config.serde;

import com.example.kstream.demo.context.ProcessContext;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class ProcessContextSerializer<I, O> implements Serializer<ProcessContext<I, O>>, Deserializer<ProcessContext<I, O>> {

    @Override
    public byte[] serialize(String topic, ProcessContext<I, O> data) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(data);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing ProcessContext", e);
        }
    }

    @Override
    public ProcessContext<I, O> deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            return (ProcessContext<I, O>) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing ProcessContext", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No es necesario implementar nada aquí para este ejemplo.
    }

    @Override
    public void close() {
        // No es necesario implementar nada aquí para este ejemplo.
    }
}
