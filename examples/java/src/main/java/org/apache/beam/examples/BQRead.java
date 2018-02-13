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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRowProto;
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
  private static final String DEFAULT_TABLE_REFERENCE = "bigquery-public-data:samples.wikipedia";

  /**
   * Options supported by {@link BQRead}.
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
      FULL,
      FIELD_SELECTION,
      FILTER
    }

    @Description("API Version to use")
    @Required
    Integer getVersion();
    void setVersion(Integer version);

    @Description("Read mode: FULL, FIELD_SELECTION, FILTER")
    @Required
    ReadMode getReadMode();
    void setReadMode(ReadMode value);
 }

  public static void main(String[] args) {
    BQReadOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(BQReadOptions.class);

    Pipeline p = Pipeline.create(options);

    // TODO: Add some data validation on the results.
    if (options.getVersion() == 3) {
      if (options.getReadMode() == BQReadOptions.ReadMode.FULL) {
        p.apply("ReadLinesV3", BigQueryIO.readTableRows(TypedRead.Method.BQ_PARALLEL_READ).from(
            DEFAULT_TABLE_REFERENCE));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.FIELD_SELECTION) {
        p.apply("ReadLinesV3PartialRow",
            BigQueryIO.readRowProtoSource(
                new SerializableFunction<SchemaAndRowProto, String>() {
                  public String apply(SchemaAndRowProto record) {
                    return (String) record.get("title");
                  }
                })
                .from(DEFAULT_TABLE_REFERENCE)
                .withCoder(NullableCoder.of(StringUtf8Coder.of()))
                .withReadSessionOptions(ReadSessionOptions.builder()
                    .addSelectedField("title")
                    .build()));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.FILTER) {
        p.apply("ReadLinesV3Filter", BigQueryIO.readTableRows(TypedRead.Method.BQ_PARALLEL_READ)
            .from(DEFAULT_TABLE_REFERENCE)
            .withReadSessionOptions(ReadSessionOptions.builder()
                .setSqlFilter("num_characters > 5000")
                .build()));
      }
    } else {
      if (options.getReadMode() == BQReadOptions.ReadMode.FULL) {
        p.apply("ReadLines",
            BigQueryIO.readTableRows().from(DEFAULT_TABLE_REFERENCE));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.FIELD_SELECTION) {
        p.apply("ReadLinesPartialRow",
            BigQueryIO.read(
                new SerializableFunction<SchemaAndRecord, String>() {
                  @Override
                  public String apply(SchemaAndRecord record) {
                    return record.getRecord().get("title").toString();
                  }
                })
                .from(DEFAULT_TABLE_REFERENCE));
      } else if (options.getReadMode() == BQReadOptions.ReadMode.FILTER) {
        // TODO: Add a meaningful example of do some computation with upper level filtering.
        p.apply("ReadLines",
            BigQueryIO.readTableRows().from(DEFAULT_TABLE_REFERENCE));
      }
    }
    p.run().waitUntilFinish();
  }
}