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

package org.apache.pig.piggybank.storage.avro;

import java.io.IOException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * This is an implementation of record reader which reads in avro data and
 * convert them into <NullWritable, Writable> pairs.
 */
public class PigAvroRecordReader extends RecordReader<NullWritable, Writable> {

    private static final Log LOG = LogFactory.getLog(PigAvroRecordReader.class);

    private AvroStorageInputStream in;
    private DataFileReader<Object> reader;   /*reader of input avro data*/
    private long start;
    private long end;
    private Path path;
    private boolean ignoreBadFiles; /* whether ignore corrupted files during load */

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    /* if multiple avro record schemas are merged, this list will hold field objects
     * of the merged schema.
     */
    private ArrayList<Object> mProtoTuple;

    /* if multiple avro record schemas are merged, this map associates each input
     * record with a remapping of its fields relative to the merged schema. please
     * see AvroStorageUtils.getSchemaToMergedSchemaMap() for more details.
     */
    private Map<Path, Map<Integer, Integer>> schemaToMergedSchemaMap;

    /**
     * constructor to initialize input and avro data reader
     */
    public PigAvroRecordReader(TaskAttemptContext context, FileSplit split,
            Schema schema, boolean ignoreBadFiles,
            Map<Path, Map<Integer, Integer>> schemaToMergedSchemaMap) throws IOException {
        this.path = split.getPath();
        this.in = new AvroStorageInputStream(path, context);
        if(schema == null) {
            AvroStorageLog.details("No avro schema given; assuming the schema is embedded");
        }

        try {
          this.reader = new DataFileReader<Object>(in, new PigAvroDatumReader(schema));
        } catch (IOException e) {
          throw new IOException("Error initializing data file reader for file (" +
              split.getPath() + ")", e);
        }
        this.reader.sync(split.getStart()); // sync to start
        this.start = in.tell();
        this.end = split.getStart() + split.getLength();
        this.ignoreBadFiles = ignoreBadFiles;
        this.schemaToMergedSchemaMap = schemaToMergedSchemaMap;
        if (schemaToMergedSchemaMap != null) {
            // initialize mProtoTuple
            int maxPos = 0;
            for (Map<Integer, Integer> map : schemaToMergedSchemaMap.values()) {
                for (Integer i : map.values()) {
                    maxPos = Math.max(i, maxPos);
                }
            }
            int tupleSize = maxPos + 1;
            AvroStorageLog.details("Creating proto tuple of fixed size: " + tupleSize);
            mProtoTuple = new ArrayList<Object>(tupleSize);
            for (int i = 0; i < tupleSize; i++) {
                mProtoTuple.add(i, null);
            }
        }
    }

    @Override
    public float getProgress() throws IOException {
        return end == start ? 0.0f : Math.min(1.0f, (getPos() - start) / (float) (end - start));
    }

    public long getPos() throws IOException {
        return in.tell();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Writable getCurrentValue() throws IOException, InterruptedException {
        Object obj = reader.next();
        Tuple result = null;
        if (obj instanceof Tuple) {
            AvroStorageLog.details("Class =" + obj.getClass());
            result = (Tuple) obj;
        } else {
            AvroStorageLog.details("Wrap calss " + obj.getClass() + " as a tuple.");
            result = wrapAsTuple(obj);
        }
        if (schemaToMergedSchemaMap != null) {
            // remap the position of fields to the merged schema
            Map<Integer, Integer> map = schemaToMergedSchemaMap.get(path);
            if (map == null) {
                throw new IOException("The schema of '" + path + "' " +
                                      "is not merged by AvroStorage.");
            }
            result = remap(result, map);
        }
        return result;
    }

    /**
     * Remap the position of fields to the merged schema
     */
    private Tuple remap(Tuple tuple, Map<Integer, Integer> map) throws IOException {
        try {
            for (int pos = 0; pos < tuple.size(); pos++) {
                mProtoTuple.set(map.get(pos), tuple.get(pos));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return tupleFactory.newTuple(mProtoTuple);
    }

    /**
     * Wrap non-tuple value as a tuple
     */
    protected Tuple wrapAsTuple(Object in) {
        Tuple tuple = tupleFactory.newTuple();
        tuple.append(in);
        return tuple;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        // Nothing to do
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (!reader.hasNext() || reader.pastSync(end)) {
                return false;
            }
            return true;
        } catch (AvroRuntimeException e) {
            if (ignoreBadFiles) {
                // For currupted files, AvroRuntimeException can be thrown.
                // We ignore them if the option 'ignore_bad_files' is enabled.
                LOG.warn("Ignoring bad file '" + path + "'.");
                return false;
            } else {
                throw e;
            }
        }
    }

}
