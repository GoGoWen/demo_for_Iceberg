package org.example;


import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.*;


/**
 * AppAvro
 * step 1: create a table with hadoopcatalog,
 * step 2: insert data with the init schema with Avro format
 * step 3: add column with default value 1
 * step 4: query the table with select *, then get the default value with newly added column
 */
public class AppAvro
{
    public static final String wareHouseLocation = "/Users/yangbowen22/work/iceberg/warehouse/avro/"+UUID.randomUUID().toString();
    //public static final String wareHouseLocation = "hdfs://ns1017/user/jd_ad/ads_report/yangbowen/warehouse/";
    public static final Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "birth", Types.StringType.get()));

    public static final Configuration hdfs_config = new Configuration();
    static {
        hdfs_config.set("dfs.nameservices","ns1017");
        hdfs_config.set("dfs.ha.namenodes.ns1017","nn1,nn2");
        hdfs_config.set("dfs.namenode.rpc-address.ns1017.nn1","10.198.37.4:8020");
        hdfs_config.set("dfs.namenode.rpc-address.ns1017.nn2","10.198.36.40:8020");
        hdfs_config.set("dfs.client.failover.proxy.provider.ns1017","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }

    public static final Configuration local_config = new Configuration();
    public static final Configuration config = local_config;

    public static final String strDb = "iceberg_db";
    public static final String strTable = "test";

    public static void main( String[] args ) throws IOException {
        //create table
        create(strDb, strTable);

        // insert data into table strDb.strTable
        {
            try {
                insert(strDb, strTable);
            } catch (IOException exception) {
                System.out.println("insert data into table:" + strDb + "." + strTable + " fail.");
                throw new IOException("insert fail");
            }

            System.out.println("insert data into table:" + strDb + "." + strTable + " succeed.");
        }

        // query data from table strDb.strTable
        {
            System.out.println("query all data: after insert data into table:" + strDb + "." + strTable);
            query(strDb, strTable);
        }

        // update schema add column with default value
        {
            updateSchema(strDb, strTable);
            System.out.println("update schema to add column with default value succeed.");
        }

        {
            System.out.println("query all data: after update schema for table with default value:" + strDb + "." + strTable);
            query(strDb, strTable);
        }

        // insert new record that default value set to another value
        {
            insertDefault(strDb, strTable);
            System.out.println("insert records with default value set to other value succeed.");
        }

        //query
        {
            System.out.println("query all data: after insert  cords with default value set to other value, for table:" + strDb + "." + strTable);
            query(strDb, strTable);
        }
    }

    public static void create(String db, String table) {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);

        // partition spec with month of birth
        // PartitionSpec spec = PartitionSpec.builderFor(schema).month("birth").build();
        // unpartioned
        PartitionSpec spec = PartitionSpec.unpartitioned();

        // database and table
        TableIdentifier name = TableIdentifier.of(db, table);

        // table properties, not use for now
        Map<String, String> properties = new HashMap<String, String>();

        // create table
        catalog.createTable(name, schema, spec, properties);
    }

    public static void insertDefault(String strDb, String strTable) throws IOException {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);
        Table table = catalog.loadTable(TableIdentifier.of(strDb, strTable));
        // generate records
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(table.schema(), strTable));
        List<GenericData.Record> records = new ArrayList<>();
        records.add(recordBuilder.set("id", 4).set("name", "name4").set("birth", "2023-01-04").set("gender", 0).build());
        records.add(recordBuilder.set("id", 5).set("name", "name5").set("birth", "2023-01-05").set("gender", 0).build());
        records.add(recordBuilder.set("id", 6).set("name", "name6").set("birth", "2023-01-06").set("gender", 0).build());
        String fileLocation = table.location().replace("file:", "") + String.format("/data/%s.avro", UUID.randomUUID().toString());
        try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation)).schema(table.schema()).named(strTable).build()) {
            for (GenericData.Record rec : records) {
                writer.add(rec);
            }
            DataFile datafile = DataFiles.builder(table.spec()).withRecordCount(3).withPath(fileLocation).withFileSizeInBytes(Files.localInput(fileLocation).getLength()).build();
            table.newAppend().appendFile(datafile).commit();
        }
    }

    public static void insert(String strDb, String strTable) throws IOException {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);
        Table table = catalog.loadTable(TableIdentifier.of(strDb, strTable));
        // generate records
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, strTable));
        List<GenericData.Record> records = new ArrayList<>();
        records.add(recordBuilder.set("id", 1).set("name", "name1").set("birth", "2023-01-01").build());
        records.add(recordBuilder.set("id", 2).set("name", "name2").set("birth", "2023-01-02").build());
        records.add(recordBuilder.set("id", 3).set("name", "name3").set("birth", "2023-01-03").build());
        String fileLocation = table.location().replace("file:", "") + String.format("/data/%s.avro", UUID.randomUUID().toString());
        try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation)).schema(schema).named(strTable).build()) {
            for (GenericData.Record rec : records) {
                writer.add(rec);
            }
            DataFile datafile = DataFiles.builder(table.spec()).withRecordCount(3).withPath(fileLocation).withFileSizeInBytes(Files.localInput(fileLocation).getLength()).build();
            table.newAppend().appendFile(datafile).commit();
        }
    }

    public static void query(String strDb, String strTable) {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);
        Table table = catalog.loadTable(TableIdentifier.of(strDb, strTable));

        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
        CloseableIterable<Record> records = scanBuilder.build();

        // query all
        int index = 0;
        for (Record record : records) {
            // show record here
            System.out.println("record :" + index + record.toString());
            index ++ ;
        }

        // query rows with prediction
        //
    }

    public static void updateSchema(String strDb, String strTable) {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);
        Table table = catalog.loadTable(TableIdentifier.of(strDb, strTable));

        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.addColumn("gender", Types.IntegerType.get(), "", 1).commit();
    }
}
