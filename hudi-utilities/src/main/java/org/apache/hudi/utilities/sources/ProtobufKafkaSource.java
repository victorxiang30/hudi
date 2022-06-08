//

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
//import org.apache.hudi.utilities.deser.KafkaProtobufSchemaDeserializer;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.schema.GrabSchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Reads protobuf serialized Kafka data, without confluent schema-registry.
 */

public class ProtobufKafkaSource extends ProtobufSource {

  private static final Logger LOG = LogManager.getLogger(ProtobufKafkaSource.class);
  // these are native kafka's config. do not change the config names.
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  // These are settings used to pass things to KafkaProtobufDeserializer
  public static final String KAFKA_PROTOBUF_VALUE_DESERIALIZER_PROPERTY_PREFIX = "hoodie.deltastreamer.source.kafka.value.deserializer.";
  public static final String KAFKA_PROTOBUF_VALUE_DESERIALIZER_SCHEMA = KAFKA_PROTOBUF_VALUE_DESERIALIZER_PROPERTY_PREFIX + "schema";

  private final KafkaOffsetGen offsetGen;
  private final HoodieDeltaStreamerMetrics metrics;
  private final GrabSchemaProvider schemaProvider;
  private final String deserializerClassName;

  public ProtobufKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                             GrabSchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics)
      throws RestClientException, IOException {
    super(props, sparkContext, sparkSession, schemaProvider);

    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    deserializerClassName = props.getString(DataSourceWriteOptions.KAFKA_PROTOBUF_VALUE_DESERIALIZER_CLASS().key(),
        DataSourceWriteOptions.KAFKA_PROTOBUF_VALUE_DESERIALIZER_CLASS().defaultValue());

    try {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName).getName());

      //TODO use schema provider
      //  if (deserializerClassName.equals(KafkaProtobufSchemaDeserializer.class.getName())) {
      //    if (schemaProvider == null) {
      //      throw new HoodieIOException("SchemaProvider has to be set to use KafkaProtobufSchemaDeserializer");
      //    }
      //    props.put(KAFKA_PROTOBUF_VALUE_DESERIALIZER_SCHEMA, schemaProvider.getSourceSchema().toString());
      //  }
    } catch (ClassNotFoundException e) {
      String error = "Could not load custom Protobuf kafka deserializer: " + deserializerClassName;
      LOG.error(error);
      throw new HoodieException(error, e);
    }

    this.schemaProvider = schemaProvider;
    this.metrics = metrics;
    offsetGen = new KafkaOffsetGen(props);
  }

  @Override
  protected InputBatch<JavaRDD<DynamicMessage>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    try {
      OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
      long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
      LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
      if (totalNewMsgs <= 0) {
        return new InputBatch<>(Option.empty(), CheckpointUtils.offsetsToStr(offsetRanges));
      }
      JavaRDD<DynamicMessage> newDataRDD = toRDD(offsetRanges);
      return new InputBatch<>(Option.of(newDataRDD), CheckpointUtils.offsetsToStr(offsetRanges));
    } catch (org.apache.kafka.common.errors.TimeoutException e) {
      throw new HoodieSourceTimeoutException("Kafka Source timed out " + e.getMessage());
    }
  }

  //TODO return message will not be general
  private JavaRDD<DynamicMessage> toRDD(OffsetRange[] offsetRanges) {
    if (deserializerClassName.equals(ByteArrayDeserializer.class.getName())) {
      if (schemaProvider == null) {
        throw new HoodieException("Please provide a valid schema provider class when use ByteArrayDeserializer!");
      }
      //TODO change to protobuf converter that converts binary to message
      final ProtobufSchema protobufSchema = (ProtobufSchema) schemaProvider.getSourceSchema();
      final Descriptors.Descriptor descriptor = protobufSchema.toDescriptor();
      return KafkaUtils.<String, byte[]>createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
          LocationStrategies.PreferConsistent()).map(obj -> (DynamicMessage) DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(obj.value())));
    } else {
      return KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
          LocationStrategies.PreferConsistent()).map(obj -> (DynamicMessage) obj.value());
    }
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (this.props.getBoolean(KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.key(), KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.defaultValue())) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}



