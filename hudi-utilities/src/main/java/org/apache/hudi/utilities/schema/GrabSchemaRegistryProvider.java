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

import io.confluent.kafka.schemaregistry.ParsedSchema;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRestClientException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

public class GrabSchemaRegistryProvider extends GrabSchemaProvider {
  private SchemaRegistryClient schemaRegistry;
  private static Logger logger = LogManager.getLogger(GrabSchemaRegistryProvider.class);

  /**
   * Configs supported.
   */
  public static class Config {

    public static final String SRC_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    public static final String TARGET_SCHEMA_REGISTRY_URL_PROP =
        "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
    public static final String SRC_SCHEMA_SUBJECT_PROPS = "hoodie.deltastreamer.schemaprovider.source.schema.subject";
    public static final int CACHED_CAPACITY_PER_SUBJECT = 100;
  }

  public GrabSchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(SchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP));
  }

  public ParsedSchema fetchSchemaFromRegistry(String registryUrl) {
    if (schemaRegistry == null) {
      schemaRegistry = new CachedSchemaRegistryClient(
          registryUrl,
          Config.CACHED_CAPACITY_PER_SUBJECT
      );
    }
    final String subjectName = config.getString(GrabSchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP);
    try {
      final SchemaMetadata metadata = schemaRegistry.getLatestSchemaMetadata(subjectName);
      final int id = metadata.getId();
      return schemaRegistry.getSchemaById(id);
    } catch (RestClientException e) {
      logger.error(String.format("Fail to retrieve schema from registry %s", e));
      throw new HoodieRestClientException(String.format("Fail to retrieve schema from registry %s", e), e);
    } catch (IOException e) {
      throw new HoodieIOException(String.format("Fail to retrieve schema from registry %s", e), e);
    }
  }

  public ProtobufSchema fetchSchemaWithInjector(String registryUrl) {
    final ProtobufSchema schemaFromRegistry = ((ProtobufSchema) (fetchSchemaFromRegistry(registryUrl)));
    return ReferenceInjector.injectResolvedReferences(schemaFromRegistry);
  }

  @Override
  public ParsedSchema getSourceSchema() {
    String registryUrl = config.getString(GrabSchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP);
    try {
      return getSchema(registryUrl);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading source schema from registry :" + registryUrl, ioe);
    }
  }

  protected void setAuthorizationHeader(String creds, HttpURLConnection connection) {
    String encodedAuth = Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
  }

  private ParsedSchema getSchema(String registryUrl) throws IOException {
    return fetchSchemaWithInjector(registryUrl);
  }

}
