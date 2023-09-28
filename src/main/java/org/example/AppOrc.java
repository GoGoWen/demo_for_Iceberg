package org.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;

import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.orc.ORC;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * AppOrc
 * step 1: create a table with hadoopcatalog,
 * step 2: insert data with the init schema with Orc format
 * step 3: add column with default value 1
 * step 4: query the table with select *, then get the default value with newly added column
 */
public class AppOrc
{
    public static final String wareHouseLocation = "/Users/yangbowen22/work/iceberg/warehouse/orc/"+UUID.randomUUID().toString();
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
    public static final String strTable = "test_orc";

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


    public static void insert(String strDb, String strTable) throws IOException {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);
        Table table = catalog.loadTable(TableIdentifier.of(strDb, strTable));
        // generate records
        GenericRecord record = GenericRecord.create(schema);
        ImmutableList.Builder<GenericRecord> build = new ImmutableList.Builder<>();
        build.add(record.copy(ImmutableMap.of("id", 1, "name", "chen", "birth", "2020-03-08")));
        build.add(record.copy(ImmutableMap.of("id", 2, "name", "yuan", "birth", "2021-03-09")));
        build.add(record.copy(ImmutableMap.of("id", 3, "name", "jie", "birth", "2023-03-10")));
        build.add(record.copy(ImmutableMap.of("id", 4, "name", "ma", "birth", "2023-03-11")));
        ImmutableList<GenericRecord> records = build.build();

        // write record to parquet file
        String filePath = table.location() + "/" + UUID.randomUUID().toString();
        OutputFile file = table.io().newOutputFile(filePath);
        DataWriter<GenericRecord> dataWriter = ORC.writeData(file)
                .schema(schema)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .overwrite()
                .withSpec(PartitionSpec.unpartitioned())
                .build();
        try {
            for (GenericRecord rec : records) {
                dataWriter.write(rec);
            }
        } finally {
            dataWriter.close();
        }

        // put file to table
        DataFile dataFile = dataWriter.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
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
    }

    public static void updateSchema(String strDb, String strTable) {
        HadoopCatalog catalog = new HadoopCatalog(config, wareHouseLocation);
        Table table = catalog.loadTable(TableIdentifier.of(strDb, strTable));

        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.addColumn("gender", Types.IntegerType.get(), "", 1).commit();
    }
}
