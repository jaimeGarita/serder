package com.api.serder.config;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.Message;

public class ProtoBufSerializer<T extends Message> implements Serializer<T>{

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            data.writeTo(outputStream);
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error al serializar mensaje protobuf", e);
        }
    }

}