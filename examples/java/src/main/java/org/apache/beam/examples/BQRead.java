/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * An example that reads wikipedia table from BigQuery V2 and V3 read API.
 *
 * <p>New Concepts:
 * You can pass in selected fields and a limited set of filters to prune read results if you are
 * using BigQuery v3 read API.
 *
 * <p>Example run command:
 * mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.BQRead
 * -Dexec.args="--runner=DataflowRunner --inputFile=pom.xml --tempLocation=gs://yiru/tmp
 * --project=bigquerytestdefault --gcpTempLocation=gs://yiru/tmp --version=3 --readFullRow=false"
 * -Pdataflow-runner
 *
 *
 */
public class BQRead {
  /**
   * Options supported by {@link WordCount}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public interface BQReadOptions extends PipelineOptions, BigQueryOptions {
    /**
     * Enumeration of different read mode.
     */
    enum ReadMode {
      Full,
      FieldSelection,
      Filter
    }
    /**
     * Don't know why this has to be here.
     */
    @Description("Input File")
    @Required
    String getInputFile();
    void setInputFile(String value);

    @Description("API Version to use")
    @Required
    Integer getVersion();
    void setVersion(Integer version);

    @Description("Read mode: fullRow, partialRow, filter")
    @Required
    ReadMode getReadMode();
    void setReadMode(ReadMode value);
 }

  public static void main(String[] args) {
    BQReadOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(BQReadOptions.class);
    System.out.println(options.toString());

    Pipeline p = Pipeline.create(options);

    if (!(options.getVersion() == 3)) {
      if (options.getReadMode() == BQReadOptions.ReadMode.Full) {
        // TODO: when published, this needs to be a public table.
        p.apply("ReadLines",
            BigQueryIO.readTableRows().from("bigquerytestdefault:samples.wikipedia"));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.FieldSelection) {
        p.apply("ReadLinesPartialRow",
            BigQueryIO.read(
                new SerializableFunction<SchemaAndRecord, String>() {
                  @Override
                  public String apply(SchemaAndRecord record) {
                    GenericRecord r = record.getRecord();
                    return r.get("title").toString();
                  }
                }).from("bigquerytestdefault:samples.wikipedia"));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.Filter) {
        // TODO: Add a meaningful example of do some computation with upper level filtering.
        p.apply("ReadLines",
            BigQueryIO.readTableRows().from("bigquerytestdefault:samples.wikipedia"));
      }
    } else {
      if (options.getReadMode() == BQReadOptions.ReadMode.Full) {
        p.apply("ReadLinesV3", BigQueryIO.readTableRowsV3().from(
            "bigquerytestdefault:samples.wikipedia"));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.FieldSelection) {
        p.apply("ReadLinesV3PartialRow",
            BigQueryIO.readV3(
                new SerializableFunction<SchemaAndRowProto, String>() {
                  public String apply(SchemaAndRowProto record) {
                    return (String) record.get("title");
                  }
                }).from("bigquerytestdefault:samples.wikipedia")
                .withCoder(NullableCoder.of(StringUtf8Coder.of()))
                .withReadOptionsV3(BigQueryIO.ReadOptionsV3.builder()
                    .addSelectedField("title")
                    .build()));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.Filter) {
        p.apply("ReadLinesV3Filter", BigQueryIO.readTableRowsV3()
            .from("bigquerytestdefault:samples.wikipedia")
            .withReadOptionsV3(BigQueryIO.ReadOptionsV3.builder()
                .setSqlFilter("num_characters > 5000")
                .build()));
      }
    }
    p.run().waitUntilFinish();
  }
}