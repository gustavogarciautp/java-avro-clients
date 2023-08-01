package com.github.mskcourse.clients.avro;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.example.Customer;
import com.example.Id;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class MSKAvroJavaProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "10");
        // avro part
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName()); // Can
        //                                                                                                                // replace
        //properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // StringSerializer.class.getName())
        // with any other
        // key serializer
        // that you may
        // use
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        // properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        // KafkaAvroSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2");
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        // properties.setProperty("basic.auth.credentials.source", "USER_INFO");
        // properties.setProperty("basic.auth.user.info",
        // "ILPFTTJKB6VX3WHZ:gVhwatImvIvB1hjiqA8Jd9D7ScAY1iBwBO+++A1ItXrrEFbXOewVIxiWRnMgAbmN");
        // properties.setProperty("schema.registry.url","https://psrc-vrpp5.us-east-2.aws.confluent.cloud");

        // opcion
        // properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true"); // If not passed, uses
                                                                                             // "false"
        // properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "my-schema"); // If
        // not passed, uses transport name (topic name in case of Kafka, or stream name
        // in case of Kinesis Data Streams)
        // properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "my-registry"); //
        // If not passed, uses "default-registry"
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
                CustomNamingStrategy.class.getName());
        // properties.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "86400000"); // If not passed, uses
        //                                                                                   // 86400000 (24 Hours)
        // properties.put(AWSSchemaRegistryConstants.CACHE_SIZE, "10"); // default value is 200
        // properties.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.BACKWARD); // Pass a
                                                                                                  // compatibility
        // mode. If not passed,
        // uses
        // Compatibility.BACKWARD
        // properties.put(AWSSchemaRegistryConstants.DESCRIPTION, "This registry is used for several purposes."); // If not
        //                                                                                                        // passed,
        //                                                                                                        // constructs
        //                                                                                                        // a
        //                                                                                                        // description
        // properties.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name()); // If
                                                                                                                         // not
                                                                                                                         // passed,
                                                                                                                         // records
                                                                                                                         // are
                                                                                                                         // sent
                                                                                                                         // uncompressed

        // La mejor práctica es registrar esquemas fuera de la aplicación cliente para
        // controlar cuándo se registran los esquemas con Schema Registry y cómo
        // evolucionan.
        // More info:
        // https://docs.confluent.io/platform/current/schema-registry/schema_registry_onprem_tutorial.html#auto-schema-registration
        // properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
        // false); // Para entornos productivos se recomienda desactivar el registro
        // automatico de schemas

        try (Producer<Id, Customer> producer = new KafkaProducer<Id, Customer>(properties)) {
            String topic = "customer-avro";

            Id id = Id.newBuilder()
                    .setUuid(UUID.randomUUID().toString())
                    .setVersion(1.0f)
                    // .setDate("04/07/2023")
                    .build();

            // copied from avro examples
            Customer customer = Customer.newBuilder()
                    .setAge(25)
                    .setFirstName("Gus")
                    .setLastName("Londono")
                    .setHeight(178f)
                    .setWeight(75f)
                    // .setAutomatedEmail(false)
                    .setTest(true)
                    .build();

            for (int i = 0; i < 100; i++) {

                ProducerRecord<Id, Customer> producerRecord = new ProducerRecord<Id, Customer>(
                        topic, id, customer);

                System.out.println(customer);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println(metadata);
                        } else {
                            exception.printStackTrace();
                        }
                    }
                });
            }

            producer.flush();
        } catch (final SerializationException e) {
            e.printStackTrace();
        }

    }
}
