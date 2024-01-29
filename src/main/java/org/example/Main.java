package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.URI, "jdbc:sqlite:sample.db");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", "admin");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
//        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://127.0.0.1:9000/test-bucket-public");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "/Users/chuanlei/iceberg-test");
        properties.put(CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("demo", properties);
        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .build();

        Namespace namespace = Namespace.of("webapp");
        TableIdentifier name = TableIdentifier.of(namespace, "logs");

        //catalog.createTable(name, schema, spec);

        List<TableIdentifier> tables = catalog.listTables(namespace);
        System.out.println(tables);

        Table t = catalog.loadTable(name);



    }
}