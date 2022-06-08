package org.apache.hudi.utilities.schema;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ReferenceInjector {
    private static final String STREAM_INFO_IMPORT_PATH = "streams/coban/stream_info.proto";
    private static final String PII_IMPORT_PATH = "streams/coban/options/v1/pii.proto";
    private static final String GOGO_IMPORT_PATH = "github.com/gogo/protobuf/gogoproto/gogo.proto";
    private static final Map<String, String> resolvedReferences = new HashMap<>();

    static {
        resolvedReferences.putAll(loadGoogleBuiltInProtobufs());
        resolvedReferences.put(STREAM_INFO_IMPORT_PATH, readProtoDependencies("/proto/coban/stream_info.proto"));
        resolvedReferences.put(PII_IMPORT_PATH, readProtoDependencies("/proto/coban/pii.proto"));
        resolvedReferences.put(GOGO_IMPORT_PATH, readProtoDependencies("/proto/gogo/gogo.proto"));
    }

    private ReferenceInjector() {
        // prevent initiation
    }

    public static ProtobufSchema injectResolvedReferences(ProtobufSchema protobufSchema) {
        return injectResolvedReferences(protobufSchema.canonicalString());
    }

    @VisibleForTesting
    static ProtobufSchema injectResolvedReferences(String schemaString) {
        return new ProtobufSchema(schemaString, Collections.emptyList(), resolvedReferences, null, null);
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
            for (String fn: files) {
                final String proto = readProtoDependencies(String.format("/%s/%s", dir, fn));
                result.put(String.format("google/protobuf/%s", fn), proto);
            }
            return Collections.unmodifiableMap(result);
        } catch (IOException e) {
            throw new RuntimeException(String.format("target directory %s cannot be found on classpath", dir), e);
        }
    }
}
