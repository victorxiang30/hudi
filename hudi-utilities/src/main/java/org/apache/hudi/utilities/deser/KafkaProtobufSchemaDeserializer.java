///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.hudi.utilities.deser;
//
//import com.google.protobuf.Descriptors;
//import com.google.protobuf.ParsedSchema;
//import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
//import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
//import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
//import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
//import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
//import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
//import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue;
//import org.apache.avro.Schema;
//import org.apache.hudi.common.config.TypedProperties;
//import org.apache.hudi.exception.HoodieException;
//import org.apache.hudi.utilities.sources.ProtobufKafkaSource;
//import org.apache.kafka.common.config.ConfigException;
//import org.apache.kafka.common.errors.InvalidConfigurationException;
//import org.apache.kafka.common.errors.SerializationException;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import org.apache.hudi.utilities.schema.ReferenceInjector;
//
///**
// * Extending {@link KafkaProtobufSchemaDeserializer} as we need to be able to inject reader schema during deserialization.
// */
//public class KafkaProtobufSchemaDeserializer extends KafkaProtobufDeserializer {
//
//  private Schema sourceSchema;
//
//  public KafkaProtobufSchemaDeserializer() {}
//
//  public KafkaProtobufSchemaDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
//    super(client, props);
//  }
//
//  @Override
//  public void configure(Map<String, ?> configs, boolean isKey) {
//    super.configure(configs, isKey);
//
//    try {
//      TypedProperties props = getConvertToTypedProperties(configs);
//      props.getstring
//      final String subjectName = props.getString(ProtobufKafkaSource.KAFKA_PROTOBUF_VALUE_DESERIALIZER_SCHEMA);
//      final SchemaMetadata metadata = schemaRegistry.getLatestSchemaMetadata(subjectName);
//      final int id = metadata.getId();
//      // TODO verify the getSchemaById. This function exist in 7.0.1. Current version is 5.3.4
//      final ProtobufSchema schemaFromRegistry = ((ProtobufSchema) schemaRegistry.getSchemaById(id));
//      ReferenceInjector.injectResolvedReferences(schemaFromRegistry);
//      // final ProtobufSchema schemaFromRegistry = ((ProtobufSchema) schemaProvider.getSourceSchema().toString());
//      // TODO convert to avro schema
//      sourceSchema = new Schema.Parser().parse(props.getString(ProtobufKafkaSource.KAFKA_PROTOBUF_VALUE_DESERIALIZER_SCHEMA));
//    } catch (Throwable e) {
//      throw new HoodieException(e);
//    }
//  }
//
//  /**
//   * We need to inject sourceSchema instead of reader schema during deserialization or later stages of the pipeline.
//   *
//   * @param includeSchemaAndVersion
//   * @param topic
//   * @param isKey
//   * @param payload
//   * @param readerSchema
//   * @return
//   * @throws SerializationException
//   */
//  @Override
//  protected Object deserialize(
//      boolean includeSchemaAndVersion,
//      String topic,
//      Boolean isKey,
//      byte[] payload,
//      Schema readerSchema)
//      throws SerializationException {
//
//    if (this.schemaRegistry == null) {
//      throw new InvalidConfigurationException("SchemaRegistryClient not found. You need to configure the deserializer or use deserializer constructor with SchemaRegistryClient.");
//    } else if (payload == null) {
//      return null;
//    } else {
//      int id = -1;
//
//      try {
//        ByteBuffer buffer = this.getByteBuffer(payload);
//        id = buffer.getInt();
//        String subject = isKey != null && !this.strategyUsesSchema(isKey) ? this.subjectName(topic, isKey, (ProtobufSchema)null) : this.getContextName(topic);
//        ProtobufSchema schema = (ProtobufSchema)this.schemaRegistry.getSchemaBySubjectAndId(subject, id);
//        MessageIndexes indexes = MessageIndexes.readFrom(buffer);
//        String name = schema.toMessageName(indexes);
//        schema = this.schemaWithName(schema, name);
//        if (includeSchemaAndVersion) {
//          subject = this.subjectName(topic, isKey, schema);
//          schema = this.schemaForDeserialize(id, schema, subject, isKey);
//          schema = this.schemaWithName(schema, name);
//        }
//
//        int length = buffer.limit() - 1 - 4;
//        int start = buffer.position() + buffer.arrayOffset();
//        Object value;
//        if (this.parseMethod != null) {
//          try {
//            value = this.parseMethod.invoke((Object)null, buffer);
//          } catch (Exception var15) {
//            throw new ConfigException("Not a valid protobuf builder", var15);
//          }
//        } else if (this.deriveType) {
//          value = this.deriveType(buffer, schema);
//        } else {
//          Descriptors.Descriptor descriptor = schema.toDescriptor();
//          if (descriptor == null) {
//            throw new SerializationException("Could not find descriptor with name " + schema.name());
//          }
//
//          value = DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(buffer.array(), start, length));
//        }
//
//        if (includeSchemaAndVersion) {
//          Integer version = this.schemaVersion(topic, isKey, id, subject, schema, value);
//          return new ProtobufSchemaAndValue(schema.copy(version), value);
//        } else {
//          return value;
//        }
//      } catch (RuntimeException | IOException var16) {
//        throw new SerializationException("Error deserializing Protobuf message for id " + id, var16);
//      } catch (RestClientException var17) {
//        throw toKafkaException(var17, "Error retrieving Protobuf schema for id " + id);
//      }
//    }
//  }
//
//  protected TypedProperties getConvertToTypedProperties(Map<String, ?> configs) {
//    TypedProperties typedProperties = new TypedProperties();
//    for (Entry<String, ?> entry : configs.entrySet()) {
//      typedProperties.put(entry.getKey(), entry.getValue());
//    }
//    return typedProperties;
//  }
//}
//
//
