package com.github.mskcourse.clients.avro.Deserializer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;

public class CustomAWSKafkaAvroDeserializer extends GlueSchemaRegistryKafkaDeserializer {

    private AtomicInteger count = new AtomicInteger();
    private AtomicLong totalDeserializationTime = new AtomicLong();

    @Override
    public Object deserialize(String topic, byte[] data) {
        int n = count.incrementAndGet();
        System.out.println(n);
        
        long startTime = System.currentTimeMillis();
        Object object = super.deserialize(topic, data);
        long endTime = System.currentTimeMillis();
        long deserializationTime = endTime - startTime;
        
        long total = totalDeserializationTime.addAndGet(deserializationTime);
        System.out.println("Tiempo de deserialización (en milisegundos): " + deserializationTime);
        System.out.println("Tiempo total de deserialización (en milisegundos): " + total);
        System.out.println("Tiempo promedio de deserialización (en milisegundos): " + total /n );
        return object;
    }
    
}
