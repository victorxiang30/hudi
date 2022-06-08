package org.apache.hudi.utilities.sources;

import com.google.protobuf.DynamicMessage;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.schema.GrabSchemaProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class ProtobufSource extends Source<JavaRDD<DynamicMessage>> {
    public ProtobufSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          GrabSchemaProvider schemaProvider) {
        super(props, sparkContext, sparkSession, schemaProvider, SourceType.PROTOBUF);
    }
}
