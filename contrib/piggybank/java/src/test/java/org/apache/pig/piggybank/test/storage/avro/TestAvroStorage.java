/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.test.storage.avro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.piggybank.storage.avro.AvroStorage;
import org.apache.pig.piggybank.storage.avro.PigSchema2Avro;
import org.apache.pig.test.Util;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAvroStorage {

    protected static final Log LOG = LogFactory.getLog(TestAvroStorage.class);

    private static PigServer pigServerLocal = null;

    final private static String basedir = "src/test/java/org/apache/pig/piggybank/test/storage/avro/avro_test_files/";

    final private static String outbasedir = "/tmp/TestAvroStorage/";

    public static final PathFilter hiddenPathFilter = new PathFilter() {
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      };

    private static String getInputFile(String file) {
        return "file://" + System.getProperty("user.dir") + "/" + basedir + file;
    }

    final private String testDir1 = getInputFile("test_dir1");
    final private String testDir1AllFiles = getInputFile("test_dir1/*");
    final private String testDir1Files123 = getInputFile("test_dir1/test_glob{1,2,3}.avro");
    final private String testDir1Files321 = getInputFile("test_dir1/test_glob{3,2,1}.avro");
    final private String testDir12AllFiles = getInputFile("{test_dir1,test_dir2}/test_glob*.avro");
    final private String testDir21AllFiles = getInputFile("{test_dir2,test_dir1}/test_glob*.avro");
    final private String testNoMatchedFiles = getInputFile("test_dir{1,2}/file_that_does_not_exist*.avro");
    final private String testArrayFile = getInputFile("test_array.avro");
    final private String testRecordFile = getInputFile("test_record.avro");
    final private String testRecordSchema = getInputFile("test_record.avsc");
    final private String testGenericUnionFile = getInputFile("test_generic_union.avro");
    final private String testRecursiveRecordInMap = getInputFile("test_recursive_record_in_map.avro");
    final private String testRecursiveRecordInArray = getInputFile("test_recursive_record_in_array.avro");
    final private String testRecursiveRecordInUnion = getInputFile("test_recursive_record_in_union.avro");
    final private String testRecursiveRecordInRecord = getInputFile("test_recursive_record_in_record.avro");
    final private String testRecursiveRecordInUnionSchema = getInputFile("test_recursive_record_in_union.avsc");
    final private String testTextFile = getInputFile("test_record.txt");
    final private String testSingleTupleBagFile = getInputFile("messages.avro");
    final private String testNoExtensionFile = getInputFile("test_no_extension");
<<<<<<< HEAD
=======
    final private String recursiveRecordInMap =
        " {" +
        "   \"type\" : \"record\"," +
        "   \"name\" : \"recursive_record\"," +
        "   \"fields\" : [ {" +
        "     \"name\" : \"id\"," +
        "     \"type\" : \"int\"" +
        "   }, {" +
        "     \"name\" : \"nested\"," +
        "     \"type\" : [ \"null\", {" +
        "       \"type\" : \"map\"," +
        "       \"values\" : \"recursive_record\"" +
        "     } ]" +
        "   } ]" +
        " }";
    final private String recursiveRecordInArray =
        " {" +
        "   \"type\" : \"record\"," +
        "   \"name\" : \"recursive_record\"," +
        "   \"fields\" : [ {" +
        "     \"name\" : \"id\"," +
        "     \"type\" : \"int\"" +
        "   }, {" +
        "     \"name\" : \"nested\"," +
        "     \"type\" : [ \"null\", {" +
        "       \"type\" : \"array\"," +
        "       \"items\" : \"recursive_record\"" +
        "     } ]" +
        "   } ]" +
        " }";
    final private String recursiveRecordInUnion =
        " {" +
        "   \"type\" : \"record\"," +
        "   \"name\" : \"recursive_record\"," +
        "   \"fields\" : [ {" +
        "     \"name\" : \"value\"," +
        "     \"type\" : \"int\"" +
        "   }, {" +
        "     \"name\" : \"next\"," +
        "     \"type\" : [ \"null\", \"recursive_record\" ]" +
        "   } ]" +
        " }";
    final private String recursiveRecordInRecord =
        " {" +
        "   \"type\" : \"record\"," +
        "   \"name\" : \"recursive_record\"," +
        "   \"fields\" : [ {" +
        "     \"name\" : \"id\"," +
        "     \"type\" : \"int\"" +
        "   }, {" +
        "     \"name\" : \"nested\"," +
        "     \"type\" : [ \"null\", {" +
        "       \"type\" : \"record\"," +
        "       \"name\" : \"nested_record\"," +
        "       \"fields\" : [ {" +
        "         \"name\" : \"value1\"," +
        "         \"type\" : \"string\"" +
        "       }, {" +
        "         \"name\" : \"next\"," +
        "         \"type\" : \"recursive_record\"" +
        "       }, {" +
        "         \"name\" : \"value2\"," +
        "         \"type\" : \"string\"" +
        "       } ]" +
        "     } ]" +
        "   } ]" +
        " }";
    final private String testCorruptedFile = getInputFile("test_corrupted_file.avro");
    final private String testMultipleSchemas1File = getInputFile("test_primitive_types/*");
    final private String testMultipleSchemas2File = getInputFile("test_complex_types/*");
>>>>>>> 9aee27cd3c9c25bfd03c57724ba7e957a1591fed

    @BeforeClass
    public static void setup() throws ExecException {
        pigServerLocal = new PigServer(ExecType.LOCAL);
        deleteDirectory(new File(outbasedir));
    }

    @AfterClass
    public static void teardown() {
        if(pigServerLocal != null) pigServerLocal.shutdown();
    }

    @Test
    public void testRecursiveRecordInMap() throws IOException {
        // Verify that recursive records in map can be loaded/saved.
        String output= outbasedir + "testRecursiveRecordInMap";
        String expected = testRecursiveRecordInMap;
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInMap +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '" + recursiveRecordInMap + "' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordInArray() throws IOException {
        // Verify that recursive records in array can be loaded/saved.
        String output= outbasedir + "testRecursiveRecordInArray";
        String expected = testRecursiveRecordInArray;
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInArray +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '" + recursiveRecordInArray + "' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordInUnion() throws IOException {
        // Verify that recursive records in union can be loaded/saved.
        String output= outbasedir + "testRecursiveRecordInUnion";
        String expected = testRecursiveRecordInUnion;
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '" + recursiveRecordInUnion + "' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordInRecord() throws IOException {
        // Verify that recursive records in record can be loaded/saved.
        String output= outbasedir + "testRecursiveRecordInRecord";
        String expected = testRecursiveRecordInRecord;
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInRecord +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '" + recursiveRecordInRecord + "' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordWithSame() throws IOException {
        // Verify that avro schema can be specified via an external avro file
        // instead of a json string.
        String output= outbasedir + "testRecursiveRecordWithSame";
        String expected = testRecursiveRecordInUnion;
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'same', '" + testRecursiveRecordInUnion + "' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordReference1() throws IOException {
        // The relation 'in' looks like this:
        //  (1,(2,(3,)))
        //  (2,(3,))
        //  (3,)
        // $0 looks like this:
        //  (1)
        //  (2)
        //  (3)
        // Avro file stored after filtering out nulls looks like this:
        //  1
        //  2
        //  3
        String output= outbasedir + "testRecursiveRecordReference1";
        String expected = basedir + "expected_testRecursiveRecordReference1.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " first = FOREACH in GENERATE $0 AS value;",
          " filtered = FILTER first BY value is not null;",
          " STORE filtered INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '\"int\"' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordReference2() throws IOException {
        // The relation 'in' looks like this:
        //  (1,(2,(3,)))
        //  (2,(3,))
        //  (3,)
        // $1.$0 looks like this:
        //  (2)
        //  (3)
        //  ()
        // Avro file stored after filtering out nulls looks like this:
        //  2
        //  3
        String output= outbasedir + "testRecursiveRecordReference2";
        String expected = basedir + "expected_testRecursiveRecordReference2.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " second = FOREACH in GENERATE $1.$0 AS value;",
          " filtered = FILTER second BY value is not null;",
          " STORE filtered INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '\"int\"' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordReference3() throws IOException {
        // The relation 'in' looks like this:
        //  (1,(2,(3,)))
        //  (2,(3,))
        //  (3,)
        // $1.$1.$0 looks like this:
        //  (3)
        //  ()
        //  ()
        // Avro file stored after filtering out nulls looks like this:
        //  3
        String output= outbasedir + "testRecursiveRecordReference3";
        String expected = basedir + "expected_testRecursiveRecordReference3.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " third = FOREACH in GENERATE $1.$1.$0 AS value;",
          " filtered = FILTER third BY value is not null;",
          " STORE filtered INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema', '\"int\"' );"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecursiveRecordWithNoAvroSchema() throws IOException {
        // Verify that recursive records cannot be stored,
        // if no avro schema is specified either via 'schema' or 'same'.
        String output= outbasedir + "testRecursiveRecordWithNoAvroSchema";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check' );"
           };
        // Since Avro schema is not specified via the 'schema' parameter, it is
        // derived from Pig schema. Job is expected to fail because this derived
        // Avro schema (bytes) is not compatible with data (tuples).
        testAvroStorage(true, queries);
    }

    @Test
    public void testRecursiveRecordWithSchemaCheck() throws IOException {
        // Verify that recursive records cannot be stored if schema check is enbled.
        String output= outbasedir + "testRecursiveWithSchemaCheck";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'schema', '" + recursiveRecordInUnion + "' );"
           };
        try {
            testAvroStorage(queries);
            Assert.fail("Negative test to test an exception. Should not be succeeding!");
        } catch (IOException e) {
            // An IOException is thrown by AvroStorage during schema check due to incompatible
            // data types.
            assertTrue(e.getMessage().contains("bytearray is not compatible with avro"));
        }
    }

    @Test
    public void testRecursiveRecordWithSchemaFile() throws IOException {
        // Verify that recursive records cannot be stored if avro schema is specified by 'schema_file'.
        String output= outbasedir + "testRecursiveWithSchemaFile";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'schema_file', '" + testRecursiveRecordInUnionSchema + "' );"
           };
        try {
            testAvroStorage(queries);
            Assert.fail("Negative test to test an exception. Should not be succeeding!");
        } catch (FrontendException e) {
            // The IOException thrown by AvroSchemaManager for recursive record is caught
            // by the Pig frontend, and FrontendException is re-thrown.
            assertTrue(e.getMessage().contains("could not instantiate 'org.apache.pig.piggybank.storage.avro.AvroStorage'"));
        }
    }

    @Test
    public void testRecursiveRecordWithData() throws IOException {
        // Verify that recursive records cannot be stored if avro schema is specified by 'data'.
        String output= outbasedir + "testRecursiveWithData";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testRecursiveRecordInUnion +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
              " 'no_schema_check'," +
              " 'data', '" + testRecursiveRecordInUnion + "' );"
           };
        try {
            testAvroStorage(queries);
            Assert.fail("Negative test to test an exception. Should not be succeeding!");
        } catch (FrontendException e) {
            // The IOException thrown by AvroSchemaManager for recursive record is caught
            // by the Pig frontend, and FrontendException is re-thrown.
            assertTrue(e.getMessage().contains("could not instantiate 'org.apache.pig.piggybank.storage.avro.AvroStorage'"));
        }
    }

    @Test
    public void testGenericUnion() throws IOException {
        // Verify that a FrontendException is thrown if schema has generic union.
        String output= outbasedir + "testGenericUnion";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testGenericUnionFile +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
          " STORE in INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
           };
        try {
            testAvroStorage(queries);
            Assert.fail("Negative test to test an exception. Should not be succeeding!");
        } catch (FrontendException e) {
            // The IOException thrown by AvroStorage for generic union is caught
            // by the Pig frontend, and FrontendException is re-thrown.
            assertTrue(e.getMessage().contains("Cannot get schema"));
        }
    }

    @Test
    public void testMultipleSchemas1() throws IOException {
        // Verify that multiple primitive types can be loaded.
        // Input Avro files have the following schemas:
        //  "int"
        //  "long"
        //  "float"
        //  "double"
        //  "string"
        //  { "type" : "enum", "name" : "foo", "symbols" : [ "6" ] }
        // Merged Avro schema looks like this:
        //  "string"
        // The relation 'in' looks like this: (order of rows can be different.)
        //  (6)
        //  (4.0)
        //  (3.0)
        //  (5)
        //  (2)
        //  (1)
        // Avro file stored after processing looks like this:
        //  "1"
        //  "2"
        //  "3.0"
        //  "4.0"
        //  "5"
        //  "6"
        String output= outbasedir + "testMultipleSchemas1";
        String expected = basedir + "expected_testMultipleSchemas1.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testMultipleSchemas1File +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ('multiple_schemas');",
          " s = FOREACH in GENERATE StringConcat($0);",
          " o = ORDER s BY $0;",
          " STORE o INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ('schema', '\"string\"');"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testMultipleSchemas2() throws IOException {
        // Verify that multiple complex types (records) can be loaded.
        // Input Avro files have the following schemas:
        //  { "type" : "record", "name" : "r", "fields" : [ { "name" : "i", "type" : "int" } ] }
        //  { "type" : "record", "name" : "r", "fields" : [ { "name" : "l", "type" : "long" } ] }
        //  { "type" : "record", "name" : "r", "fields" : [ { "name" : "f", "type" : "float" } ] }
        //  { "type" : "record", "name" : "r", "fields" : [ { "name" : "d", "type" : "double" } ] }
        //  { "type" : "record", "name" : "r", "fields" : [ { "name" : "s", "type" : "string" } ] }
        //  { "type" : "record", "name" : "r", "fields" : [ { "name" : "e", "type" : {
        //      "type" : "enum", "name" : "foo", "symbols" : [ "6" ] } } ] }
        // Merged Avro schema looks like this:
        //  { "type" : "record",
        //    "name" : "merged",
        //    "fields" : [ { "name" : "i", "type" : "int" },
        //                 { "name" : "l", "type" : "long" },
        //                 { "name" : "f", "type" : "float" },
        //                 { "name" : "d", "type" : "double" },
        //                 { "name" : "s", "type" : "string" },
        //                 { "name" : "e", "type" : {
        //                      "type" : "enum", "name" : "foo", "symbols" : [ "6" ] } }
        //               ]
        //  }
        // The relation 'in' looks like this: (order of rows can be different.)
        //  (,,6,,,)
        //  (,,,,4.0,)
        //  (,,,,,3.0)
        //  (,5,,,,)
        //  (,,,2,,)
        //  (1,,,,,)
        // Avro file stored after processing looks like this:
        //  "1"
        //  "2"
        //  "3.0"
        //  "4.0"
        //  "5"
        //  "6"
        String output= outbasedir + "testMultipleSchemas2";
        String expected = basedir + "expected_testMultipleSchemas2.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " in = LOAD '" + testMultipleSchemas2File +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ('multiple_schemas');",
          " f = FOREACH in GENERATE ($0 is not null ? (chararray)$0 : '')," +
          "                         ($1 is not null ? (chararray)$1 : '')," +
          "                         ($2 is not null ? (chararray)$2 : '')," +
          "                         ($3 is not null ? (chararray)$3 : '')," +
          "                         ($4 is not null ? (chararray)$4 : '')," +
          "                         ($5 is not null ? (chararray)$5 : '');",
          " c = FOREACH f GENERATE StringConcat( $0, $1, $2, $3, $4, $5 );",
          " o = ORDER c BY $0;",
          " STORE o INTO '" + output +
              "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ('schema', '\"string\"');"
           };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testDir() throws IOException {
        // Verify that all files in a directory including its sub-directories are loaded.
        String output= outbasedir + "testDir";
        String expected = basedir + "expected_testDir.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testDir1 + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testGlob1() throws IOException {
        // Verify that the a glob pattern matches files properly.
        String output = outbasedir + "testGlob1";
        String expected = basedir + "expected_testDir.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testDir1AllFiles + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testGlob2() throws IOException {
        // Verify that comma-separated filenames are escaped properly.
        String output = outbasedir + "testGlob2";
        String expected = basedir + "expected_test_dir_1.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testDir1Files123 + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testGlob3() throws IOException {
        // Verify that comma-separated filenames are escaped properly.
        String output = outbasedir + "testGlob3";
        String expected = basedir + "expected_test_dir_1.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testDir1Files321 + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testGlob4() throws IOException {
        // Verify that comma-separated directory names are escaped properly.
        String output = outbasedir + "testGlob4";
        String expected = basedir + "expected_test_dir_1_2.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testDir12AllFiles + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testGlob5() throws IOException {
        // Verify that comma-separated directory names are escaped properly.
        String output = outbasedir + "testGlob5";
        String expected = basedir + "expected_test_dir_1_2.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testDir21AllFiles + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testGlob6() throws IOException {
        // Verify that an IOException is thrown if no files are matched by the glob pattern.
        String output = outbasedir + "testGlob6";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testNoMatchedFiles + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        try {
            testAvroStorage(queries);
            Assert.fail("Negative test to test an exception. Should not be succeeding!");
        } catch (JobCreationException e) {
            // The IOException thrown by AvroStorage for input file not found is catched
            // by the Pig backend, and JobCreationException (a subclass of IOException)
            // is re-thrown while creating a job configuration.
            assertEquals(e.getMessage(), "Internal error creating job configuration.");
        }
    }

    @Test
    public void testArrayDefault() throws IOException {
        String output= outbasedir + "testArrayDefault";
        String expected = basedir + "expected_testArrayDefault.avro";

        deleteDirectory(new File(output));

        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }


    @Test
    public void testArrayWithSchema() throws IOException {
        String output= outbasedir + "testArrayWithSchema";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output +
               "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
               "   'schema', '{\"type\":\"array\",\"items\":\"float\"}'  );"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testArrayWithNotNull() throws IOException {
        String output= outbasedir + "testArrayWithNotNull";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output +
               "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
               "   '{\"nullable\": false }'  );"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testArrayWithSame() throws IOException {
        String output= outbasedir + "testArrayWithSame";
        String expected = basedir + "expected_testArrayWithSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output +
               "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ( "  +
               "   'same', '" + testArrayFile + "'  );"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    @Test
    public void testArrayWithSnappyCompression() throws IOException {
        String output= outbasedir + "testArrayWithSnappyCompression";
        String expected = basedir + "expected_testArrayDefault.avro";

        deleteDirectory(new File(output));

        Properties properties = new Properties();
        properties.setProperty("mapred.output.compress", "true");
        properties.setProperty("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        properties.setProperty("avro.output.codec", "snappy");
        PigServer pigServer = new PigServer(ExecType.LOCAL, properties);
        pigServer.setBatchOn();
        String [] queries = {
           " in = LOAD '" + testArrayFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
            };
        for (String query: queries){
            pigServer.registerQuery(query);
        }
        pigServer.executeBatch();
        verifyResults(output, expected, "snappy");
    }

    @Test
    public void testRecordWithSplit() throws IOException {
        PigSchema2Avro.setTupleIndex(0);
        String output1= outbasedir + "testRecordSplit1";
        String output2= outbasedir + "testRecordSplit2";
        String expected1 = basedir + "expected_testRecordSplit1.avro";
        String expected2 = basedir + "expected_testRecordSplit2.avro";
        deleteDirectory(new File(output1));
        deleteDirectory(new File(output2));
        String [] queries = {
           " avro = LOAD '" + testRecordFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " groups = GROUP avro BY member_id;",
           " sc = FOREACH groups GENERATE group AS key, COUNT(avro) AS cnt;",
           " STORE sc INTO '" + output1 + "' " +
                 " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                 "'{\"index\": 1, " +
                 "  \"schema\": {\"type\":\"record\", " +
                                        " \"name\":\"result\", " +
                                       "  \"fields\":[ {\"name\":\"member_id\",\"type\":\"int\"}, " +
                                                             "{\"name\":\"count\", \"type\":\"long\"} " +
                                                          "]" +
                                         "}" +
                " }');",
            " STORE sc INTO '" + output2 +
                    " 'USING org.apache.pig.piggybank.storage.avro.AvroStorage ('index', '2');"
            };
        testAvroStorage( queries);
        verifyResults(output1, expected1);
        verifyResults(output2, expected2);
    }

    @Test
    public void testRecordWithSplitFromText() throws IOException {
        PigSchema2Avro.setTupleIndex(0);
        String output1= outbasedir + "testRecordSplitFromText1";
        String output2= outbasedir + "testRecordSplitFromText2";
        String expected1 = basedir + "expected_testRecordSplitFromText1.avro";
        String expected2 = basedir + "expected_testRecordSplitFromText2.avro";
        deleteDirectory(new File(output1));
        deleteDirectory(new File(output2));
        String [] queries = {
           " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
           " groups = GROUP avro BY member_id;",
           " sc = FOREACH groups GENERATE group AS key, COUNT(avro) AS cnt;",
           " STORE sc INTO '" + output1 + "' " +
                 " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                 "'{\"index\": 1, " +
                 "  \"schema\": {\"type\":\"record\", " +
                                        " \"name\":\"result\", " +
                                        " \"fields\":[ {\"name\":\"member_id\",\"type\":\"int\"}, " +
                                                      "{\"name\":\"count\", \"type\":\"long\"} " +
                                                          "]" +
                                         "}" +
                " }');",
            " STORE sc INTO '" + output2 +
                    " 'USING org.apache.pig.piggybank.storage.avro.AvroStorage ('index', '2');"
            };
        testAvroStorage( queries);
        verifyResults(output1, expected1);
        verifyResults(output2, expected2);
    }

    @Test
    public void testRecordWithFieldSchema() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchema";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " avro = LOAD '" + testRecordFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " avro1 = FILTER avro BY member_id > 1211;",
           " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
           " STORE avro2 INTO '" + output + "' " +
                 " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                 "'{\"data\":  \"" + testRecordFile + "\" ," +
                 "  \"field0\": \"int\", " +
                  " \"field1\":  \"def:browser_id\", " +
                 "  \"field3\": \"def:act_content\" " +
                " }');"
            };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecordWithFieldSchemaFromText() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchemaFromText";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
          " avro1 = FILTER avro BY member_id > 1211;",
          " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
          " STORE avro2 INTO '" + output + "' " +
                " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                "'{\"data\":  \"" + testRecordFile + "\" ," +
                "  \"field0\": \"int\", " +
                 " \"field1\":  \"def:browser_id\", " +
                "  \"field3\": \"def:act_content\" " +
               " }');"
           };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testRecordWithFieldSchemaFromTextWithSchemaFile() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchemaFromTextWithSchemaFile";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
          " avro1 = FILTER avro BY member_id > 1211;",
          " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
          " STORE avro2 INTO '" + output + "' " +
                " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                "'{\"schema_file\":  \"" + testRecordSchema + "\" ," +
                "  \"field0\": \"int\", " +
                 " \"field1\":  \"def:browser_id\", " +
                "  \"field3\": \"def:act_content\" " +
               " }');"
           };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }
    
    @Test
    public void testSingleFieldTuples() throws IOException {
        String output= outbasedir + "testSingleFieldTuples";
        String expected = basedir + "expected_testSingleFieldTuples.avro";
        deleteDirectory(new File(output));
        String [] queries = {
                " messages = LOAD '" + testSingleTupleBagFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
                " a = foreach (group messages by user_id) { sorted = order messages by message_id DESC; GENERATE group AS user_id, sorted AS messages; };",
                " STORE a INTO '" + output + "' " +
                        " USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
        };
        testAvroStorage( queries);
    }
    
    @Test
    public void testFileWithNoExtension() throws IOException {
        PigSchema2Avro.setTupleIndex(4);
        String output= outbasedir + "testFileWithNoExtension";
        String expected = basedir + "expected_testFileWithNoExtension.avro";
        deleteDirectory(new File(output));
        String [] queries = {
                " avro = LOAD '" + testNoExtensionFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
                " avro1 = FILTER avro BY member_id > 1211;",
                " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
                " STORE avro2 INTO '" + output + "' " +
                        " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                        "'{\"data\":  \"" + testNoExtensionFile + "\" ," +
                        "  \"field0\": \"int\", " +
                        " \"field1\":  \"def:browser_id\", " +
                        "  \"field3\": \"def:act_content\" " +
                        " }');"
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testSingleFieldTuples() throws IOException {
        String output= outbasedir + "testSingleFieldTuples";
        String expected = basedir + "expected_testSingleFieldTuples.avro";
        deleteDirectory(new File(output));
        String [] queries = {
                " messages = LOAD '" + testSingleTupleBagFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
                " a = foreach (group messages by user_id) { sorted = order messages by message_id DESC; GENERATE group AS user_id, sorted AS messages; };",
                " STORE a INTO '" + output + "' " +
                        " USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
        };
        testAvroStorage( queries);
    }

    @Test
    public void testFileWithNoExtension() throws IOException {
        PigSchema2Avro.setTupleIndex(4);
        String output= outbasedir + "testFileWithNoExtension";
        String expected = basedir + "expected_testFileWithNoExtension.avro";
        deleteDirectory(new File(output));
        String [] queries = {
                " avro = LOAD '" + testNoExtensionFile + " ' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
                " avro1 = FILTER avro BY member_id > 1211;",
                " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
                " STORE avro2 INTO '" + output + "' " +
                        " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                        "'{\"data\":  \"" + testNoExtensionFile + "\" ," +
                        "  \"field0\": \"int\", " +
                        " \"field1\":  \"def:browser_id\", " +
                        "  \"field3\": \"def:act_content\" " +
                        " }');"
        };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    // Same as above, just without using json in the constructor
    @Test
    public void testRecordWithFieldSchemaFromTextWithSchemaFile2() throws IOException {
        PigSchema2Avro.setTupleIndex(1);
        String output= outbasedir + "testRecordWithFieldSchemaFromTextWithSchemaFile2";
        String expected = basedir + "expected_testRecordWithFieldSchema.avro";
        deleteDirectory(new File(output));
        String [] queries = {
          " avro = LOAD '" + testTextFile + "' AS (member_id:int, browser_id:chararray, tracking_time:long, act_content:bag{inner:tuple(key:chararray, value:chararray)});",
          " avro1 = FILTER avro BY member_id > 1211;",
          " avro2 = FOREACH avro1 GENERATE member_id, browser_id, tracking_time, act_content ;",
          " STORE avro2 INTO '" + output + "' " +
                " USING org.apache.pig.piggybank.storage.avro.AvroStorage (" +
                "'schema_file', '" + testRecordSchema + "'," +
                "'field0','int'," +
                "'field1','def:browser_id'," +
                "'field3','def:act_content'" +
                ");"
           };
        testAvroStorage( queries);
        verifyResults(output, expected);
    }

    @Test
    public void testCorruptedFile1() throws IOException {
        // Verify that load fails when bad files are found if ignore_bad_files is disabled.
        String output = outbasedir + "testCorruptedFile1";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testCorruptedFile + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
            };
        // Job is expected to fail for bad files.
        testAvroStorage(true, queries);
    }

    @Test
    public void testCorruptedFile2() throws IOException {
        // Verify that corrupted files are skipped if ignore_bad_files is enabled.
        // Output is expected to be empty.
        String output = outbasedir + "testCorruptedFile2";
        String expected = basedir + "expected_testCorruptedFile.avro";
        deleteDirectory(new File(output));
        String [] queries = {
           " in = LOAD '" + testCorruptedFile + "'" +
                  " USING org.apache.pig.piggybank.storage.avro.AvroStorage ('ignore_bad_files');",
           " STORE in INTO '" + output + "' USING org.apache.pig.piggybank.storage.avro.AvroStorage ();"
            };
        testAvroStorage(queries);
        verifyResults(output, expected);
    }

    private static void deleteDirectory (File path) {
        if ( path.exists()) {
            File [] files = path.listFiles();
            for (File file: files) {
                if (file.isDirectory())
                    deleteDirectory(file);
                file.delete();
            }
        }
    }

    private void testAvroStorage(String ...queries) throws IOException {
        testAvroStorage(false, queries);
    }

    private void testAvroStorage(boolean expectedToFail, String ...queries) throws IOException {
        pigServerLocal.setBatchOn();
        for (String query: queries){
            if (query != null && query.length() > 0) {
                pigServerLocal.registerQuery(query);
            }
        }
        int numOfFailedJobs = 0;
        for (ExecJob job : pigServerLocal.executeBatch()) {
            if (job.getStatus().equals(JOB_STATUS.FAILED)) {
                numOfFailedJobs++;
            }
        }
        if (expectedToFail) {
            assertTrue("There was no failed job!", numOfFailedJobs > 0);
        } else {
            assertTrue("There was a failed job!", numOfFailedJobs == 0);
        }
    }

    private void verifyResults(String outPath, String expectedOutpath) throws IOException {
        verifyResults(outPath, expectedOutpath, null);
    }

    private void verifyResults(String outPath, String expectedOutpath, String expectedCodec) throws IOException {
        // Seems compress for Avro is broken in 23. Skip this test and open Jira PIG-
        if (Util.isHadoop23())
            return;
<<<<<<< HEAD
        
        FileSystem fs = FileSystem.getLocal(new Configuration()) ; 
        
=======

        FileSystem fs = FileSystem.getLocal(new Configuration()) ;

>>>>>>> 9aee27cd3c9c25bfd03c57724ba7e957a1591fed
        /* read in expected results*/
        Set<Object> expected = getExpected (expectedOutpath);

        /* read in output results and compare */
        Path output = new Path(outPath);
        assertTrue("Output dir does not exists!", fs.exists(output)
                && fs.getFileStatus(output).isDir());

        Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
        assertTrue("Split field dirs not found!", paths != null);

        for (Path path : paths) {
          Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
          assertTrue("No files found for path: " + path.toUri().getPath(),
                  files != null);
          for (Path filePath : files) {
            assertTrue("This shouldn't be a directory", fs.isFile(filePath));

            GenericDatumReader<Object> reader = new GenericDatumReader<Object>();

            DataFileStream<Object> in = new DataFileStream<Object>(
                                            fs.open(filePath), reader);
            assertEquals("codec", expectedCodec, in.getMetaString("avro.codec"));
            int count = 0;
            while (in.hasNext()) {
                Object obj = in.next();
                //System.out.println("obj = " + (GenericData.Array<Float>)obj);
                assertTrue("Avro result object found that's not expected: " + obj, expected.contains(obj));
                count++;
            }
            in.close();
            assertEquals(expected.size(), count);
          }
        }
      }

    private Set<Object> getExpected (String pathstr ) throws IOException {

        Set<Object> ret = new HashSet<Object>();
        FileSystem fs = FileSystem.getLocal(new Configuration());

        /* read in output results and compare */
        Path output = new Path(pathstr);
        assertTrue("Expected output does not exists!", fs.exists(output));

        Path[] paths = FileUtil.stat2Paths(fs.listStatus(output, hiddenPathFilter));
        assertTrue("Split field dirs not found!", paths != null);

        for (Path path : paths) {
            Path[] files = FileUtil.stat2Paths(fs.listStatus(path, hiddenPathFilter));
            assertTrue("No files found for path: " + path.toUri().getPath(), files != null);
            for (Path filePath : files) {
                assertTrue("This shouldn't be a directory", fs.isFile(filePath));

                GenericDatumReader<Object> reader = new GenericDatumReader<Object>();

                DataFileStream<Object> in = new DataFileStream<Object>(fs.open(filePath), reader);

                while (in.hasNext()) {
                    Object obj = in.next();
                    ret.add(obj);
                }
                in.close();
            }
        }
        return ret;
  }

}
