package com.github.mskcourse.clients.avro;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.example.Customer;
import com.github.mskcourse.clients.avro.Deserializer.CustomAWSKafkaAvroDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class MSKAvroJavaConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal consumer
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "customer-consumer-group");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // avro part (deserializer)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        
        
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomAWSKafkaAvroDeserializer.class.getName());
        
        
        properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2"); // Pass an AWS Region
        //properties.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, "my-registry");

        properties.setProperty(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName()); // Only required for AVRO data format

        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "https://psrc-vrpp5.us-east-2.aws.confluent.cloud");
        properties.setProperty(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, KafkaAvroDeserializer.class.getName()); // For migration fall back scenario
        properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        // properties.setProperty("basic.auth.credentials.source", "USER_INFO");
        // properties.setProperty("basic.auth.user.info", "ILPFTTJKB6VX3WHZ:gVhwatImvIvB1hjiqA8Jd9D7ScAY1iBwBO+++A1ItXrrEFbXOewVIxiWRnMgAbmN");

        
        // Optional
        properties.setProperty(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "120000"); // If not passed, uses 86400000
        properties.setProperty(AWSSchemaRegistryConstants.CACHE_SIZE, "100"); // default value is 200


        try (final KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(properties)) {
            String topic = "customer-avro";
            kafkaConsumer.subscribe(Collections.singleton(topic));

            System.out.println("Waiting for data...");

            while (true){
                System.out.println("Polling...");
                
                final ConsumerRecords<String, Customer> records = kafkaConsumer.poll(Duration.ofMillis(5000));

                for (ConsumerRecord<String, Customer> record : records){
                        Customer customer = record.value();
                        System.out.println(customer);
                }

                kafkaConsumer.commitSync();
            }
        }
    }
}
