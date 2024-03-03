package com.api.serder.config;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;

public class ProtoBufDeserializer<T extends Message> implements Deserializer<T> {

    private final Class<T> messageType;

    public ProtoBufDeserializer(Class<T> messageType) {
        this.messageType = messageType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
        
            java.lang.reflect.Method getDefaultInstanceMethod = messageType.getMethod("getDefaultInstance");

            Object defaultInstance = getDefaultInstanceMethod.invoke(null);
            java.lang.reflect.Method newBuilderMethod = defaultInstance.getClass().getMethod("newBuilder");
            MessageLite.Builder builder = (MessageLite.Builder) newBuilderMethod.invoke(defaultInstance);

            // Fusionamos los datos binarios en el constructor y construimos el mensaje
            return (T) builder.mergeFrom(data).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
