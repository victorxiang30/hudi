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

package org.apache.hudi.utilities.schema;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;

public class ReferenceInjector {
  private static final String STREAM_INFO_IMPORT_PATH = "streams/coban/stream_info.proto";
  private static final String PII_IMPORT_PATH = "streams/coban/options/v1/pii.proto";
  private static final String GOGO_IMPORT_PATH = "github.com/gogo/protobuf/gogoproto/gogo.proto";
  private static final Map<String, String> RESOLVED_REFERENCES = new HashMap<>();

  static {
    RESOLVED_REFERENCES.putAll(loadGoogleBuiltInProtobufs());
    RESOLVED_REFERENCES.put(STREAM_INFO_IMPORT_PATH, readProtoDependencies("/proto/coban/stream_info.proto"));
    RESOLVED_REFERENCES.put(PII_IMPORT_PATH, readProtoDependencies("/proto/coban/pii.proto"));
    RESOLVED_REFERENCES.put(GOGO_IMPORT_PATH, readProtoDependencies("/proto/gogo/gogo.proto"));
  }

  private ReferenceInjector() {
    // prevent initiation
  }

  public static ProtobufSchema injectResolvedReferences(ProtobufSchema protobufSchema) {
    return injectResolvedReferences(protobufSchema.canonicalString());
  }

  static ProtobufSchema injectResolvedReferences(String schemaString) {
    return new ProtobufSchema(schemaString, Collections.emptyList(), RESOLVED_REFERENCES, null, null);
  }

  private static String readProtoDependencies(String fullPath) {
    try {
      return IOUtils.toString(Objects.requireNonNull(
              ReferenceInjector.class.getResourceAsStream(fullPath)),
          StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(String.format("target file %s cannot be found on classpath", fullPath), e);
    }
  }

  private static Map<String, String> loadGoogleBuiltInProtobufs() {
    final String dir = "proto/google/";
    try {
      final List<String> files = IOUtils.readLines(
          Objects.requireNonNull(ReferenceInjector.class.getClassLoader().getResourceAsStream(dir)),
          StandardCharsets.UTF_8);
      final Map<String, String> result = new HashMap<>();
      for (String fn : files) {
        final String proto = readProtoDependencies(String.format("/%s/%s", dir, fn));
        result.put(String.format("google/protobuf/%s", fn), proto);
      }
      return Collections.unmodifiableMap(result);
    } catch (IOException e) {
      throw new RuntimeException(String.format("target directory %s cannot be found on classpath", dir), e);
    }
  }
}
