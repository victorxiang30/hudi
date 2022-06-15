/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Collection;
import java.util.Map;

public class GrabHelper {
  private static Message unwrapMessageIfInBag(Message message) {
    final Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();
    if (allFields.size() != 1) {
      return message;
    }

    final boolean isBagInName = allFields.keySet().stream().anyMatch(d -> d.getFullName().contains("Bag."));
    final Object singletonValue = allFields.values().iterator().next();

    if (isBagInName && singletonValue instanceof Collection) {
      return (Message) ((Collection<?>) singletonValue).iterator().next();
    }

    return message;
  }

  public static SchemaAndValue toInnerSchemaAndValue(ProtobufSchema schema, Message message, ProtobufData protobufData) {
    final Message inner = unwrapMessageIfInBag(message);
    final Descriptors.Descriptor innerDescriptor = inner.getDescriptorForType();
    final ProtobufSchema innerProtobufSchema = new ProtobufSchema(
        innerDescriptor,
        schema.references());
    return protobufData.toConnectData(innerProtobufSchema, inner);
  }
}
