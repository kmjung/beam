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

import com.google.common.collect.Lists;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRowProto;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * An example that counts edits to Wikipedia and includes best practices for reading from BigQuery.
 *
 * <p>This class, {@link BigQueryWordCount}, is an addendum to the standard WordCount demos in the
 * Beam repository. It demonstrates how to read data from BigQuery using the existing {@link
 * BigQueryIO.Read} implementation, which exports data to Google Cloud Storage in Apache Avro format
 * for processing, and shows how to build identical pipelines using the new BigQuery parallel read
 * API while optimizing the amount of data to be processed using features of the new API such as
 * column selection and push-down SQL filtering.
 */
public class BigQueryWordCount {

  private static final String DEFAULT_TABLE_REFERENCE = "bigquery-public-data:samples.wikipedia";
  private static final String NUM_CHARACTERS_FIELD_NAME = "num_characters";
  private static final String CONTRIBUTOR_USERNAME_FIELD_NAME = "contributor_username";
  private static final String CONTRIBUTOR_IP_FIELD_NAME = "contributor_ip";

  private static final String FILTER_PROJECTION_QUERY_STRING =
      "#standardSQL\n"
          + "SELECT contributor_username, contributor_ip\n"
          + "FROM `bigquery-public-data.samples.wikipedia`\n"
          + "WHERE num_characters > 5000";

  /**
   * A class representing the interesting fields from an entry in the Wikipedia edit table. In
   * practice, this is usually a user-defined or domain-specific proto object.
   */
  static class TrimmedEditRecord implements Serializable {
    Long numCharacters;
    String contributorUsername;
    String contributorIp;
  }

  /**
   * A {@link SerializableFunction} which parses a {@link SchemaAndRecord} object into a {@link
   * TrimmedEditRecord}.
   */
  static SerializableFunction<SchemaAndRecord, TrimmedEditRecord> parseRecordFn =
      (input) -> {
        TrimmedEditRecord editRecord = new TrimmedEditRecord();
        editRecord.numCharacters = (Long) input.getRecord().get(NUM_CHARACTERS_FIELD_NAME);
        // Avro can prefer other CharSequence types (e.g. org.apache.avro.util.Utf8) to Java String.
        CharSequence contributorUsername =
            (CharSequence) input.getRecord().get(CONTRIBUTOR_USERNAME_FIELD_NAME);
        if (contributorUsername != null) {
          editRecord.contributorUsername = contributorUsername.toString();
        }
        CharSequence contributorIp =
            (CharSequence) input.getRecord().get(CONTRIBUTOR_IP_FIELD_NAME);
        if (contributorIp != null) {
          editRecord.contributorIp = contributorIp.toString();
        }
        return editRecord;
      };

  /**
   * A {@link SerializableFunction} which parses a {@link SchemaAndRowProto} object into a {@link
   * TrimmedEditRecord}.
   */
  static SerializableFunction<SchemaAndRowProto, TrimmedEditRecord> parseRowProtoFn =
      (input) -> {
        TrimmedEditRecord editRecord = new TrimmedEditRecord();
        editRecord.numCharacters = (Long) input.get(NUM_CHARACTERS_FIELD_NAME);
        editRecord.contributorUsername = (String) input.get(CONTRIBUTOR_USERNAME_FIELD_NAME);
        editRecord.contributorIp = (String) input.get(CONTRIBUTOR_IP_FIELD_NAME);
        return editRecord;
      };

  /**
   * A {@link DoFn} which takes as input a collection of {@link TrimmedEditRecord} objects
   * representing rows in a table of Wikipedia edit records and produces as output a collection of
   * the same objects with edits to pages of 5000 characters or less removed.
   */
  static class FilterSmallPagesFn extends DoFn<TrimmedEditRecord, TrimmedEditRecord> {
    private final Counter filteredEdits =
        Metrics.counter(FilterSmallPagesFn.class, "Edits to pages of 5000 characters or less");

    @ProcessElement
    public void processElement(ProcessContext c) {
      TrimmedEditRecord editRecord = c.element();
      if (editRecord.numCharacters != null && editRecord.numCharacters > 5000) {
        c.output(editRecord);
      } else {
        filteredEdits.inc();
      }
    }
  }

  /**
   * A {@link SimpleFunction} which transforms a collection of {@link TrimmedEditRecord} objects
   * representing rows in a table of Wikipedia edit records and produces as output a collection of
   * the usernames or IP addresses of the edit authors in string form.
   */
  static class ExtractUserInfoFn extends DoFn<TrimmedEditRecord, String> {
    private final Counter editsByUnknownAuthor =
        Metrics.counter(
            ExtractUserInfoFn.class,
            "Edits whose author (contributor username or IP address) is unknown");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String contributorUsername = c.element().contributorUsername;
      if (contributorUsername != null && !contributorUsername.isEmpty()) {
        c.output(contributorUsername);
        return;
      }

      String contributorIp = c.element().contributorIp;
      if (contributorIp != null && !contributorIp.isEmpty()) {
        c.output(contributorIp);
        return;
      }

      editsByUnknownAuthor.inc();
    }
  }

  /** Options supported by {@link BigQueryWordCount}. */
  public interface BigQueryWordCountOptions extends org.apache.beam.sdk.options.PipelineOptions {

    /** Enumerates the possible pipeline types supported by this test. */
    enum PipelineType {
      EXPORT_TABLE_DATA,
      EXPORT_QUERY_RESULTS,
      READ_TABLE_DATA,
      READ_TABLE_DATA_AND_FILTER,
      READ_QUERY_RESULTS,
      READ_TABLE_DATA_AS_ROW_PROTO,
      READ_TABLE_DATA_AS_ROW_PROTO_AND_FILTER,
      READ_QUERY_RESULTS_AS_ROW_PROTO,
    }

    @Description(
        "Pipeline type: "
            + "EXPORT_TABLE_DATA, "
            + "EXPORT_QUERY_RESULTS, "
            + "READ_TABLE_DATA, "
            + "READ_TABLE_DATA_AND_FILTER, "
            + "READ_QUERY_RESULTS, "
            + "READ_TABLE_DATA_AS_ROW_PROTO, "
            + "READ_TABLE_DATA_AS_ROW_PROTO_AND_FILTER, "
            + "READ_QUERY_RESULTS_AS_ROW_PROTO")
    @Required
    PipelineType getPipelineType();

    void setPipelineType(PipelineType pipelineType);
  }

  static void runBQWordCount(BigQueryWordCountOptions options) {
    Pipeline p = Pipeline.create(options);
    PCollection<TrimmedEditRecord> editRecords;
    switch (options.getPipelineType()) {
      case EXPORT_TABLE_DATA:
        editRecords =
            p.apply(
                    "ExtractTableDataAndReadRows",
                    BigQueryIO.read(parseRecordFn)
                        .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                        .from(DEFAULT_TABLE_REFERENCE))
                .apply("FilterEditsToSmallArticles", ParDo.of(new FilterSmallPagesFn()));
        break;
      case EXPORT_QUERY_RESULTS:
        editRecords =
            p.apply(
                "ExtractQueryResultsAndReadRows",
                BigQueryIO.read(parseRecordFn)
                    .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                    .fromQuery(FILTER_PROJECTION_QUERY_STRING)
                    .usingStandardSql());
        break;
      case READ_TABLE_DATA:
        editRecords =
            p.apply(
                    "ReadTableData",
                    BigQueryIO.read(parseRecordFn)
                        .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                        .withMethod(Method.READ)
                        .from(DEFAULT_TABLE_REFERENCE))
                .apply("FilterEditsToSmallArticles", ParDo.of(new FilterSmallPagesFn()));
        break;
      case READ_TABLE_DATA_AND_FILTER:
        editRecords =
            p.apply(
                "ReadTableDataAndFilter",
                BigQueryIO.read(parseRecordFn)
                    .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                    .withMethod(Method.READ)
                    .withReadSessionOptions(
                        ReadSessionOptions.builder()
                            .setSqlFilter(NUM_CHARACTERS_FIELD_NAME + " > 5000")
                            .setSelectedFields(
                                Lists.newArrayList(
                                    NUM_CHARACTERS_FIELD_NAME,
                                    CONTRIBUTOR_USERNAME_FIELD_NAME,
                                    CONTRIBUTOR_IP_FIELD_NAME))
                            .build())
                    .from(DEFAULT_TABLE_REFERENCE));
        break;
      case READ_QUERY_RESULTS:
        editRecords =
            p.apply(
                "ReadQueryResults",
                BigQueryIO.read(parseRecordFn)
                    .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                    .withMethod(Method.READ)
                    .fromQuery(FILTER_PROJECTION_QUERY_STRING)
                    .usingStandardSql());
        break;
      case READ_TABLE_DATA_AS_ROW_PROTO:
        editRecords =
            p.apply(
                    "ReadTableDataAsRowProto",
                    BigQueryIO.readViaRowProto(parseRowProtoFn)
                        .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                        .from(DEFAULT_TABLE_REFERENCE))
                .apply("FilterEditsToSmallArticles", ParDo.of(new FilterSmallPagesFn()));
        break;
      case READ_TABLE_DATA_AS_ROW_PROTO_AND_FILTER:
        editRecords =
            p.apply(
                "ReadTableDataAsRowProtoAndFilter",
                BigQueryIO.readViaRowProto(parseRowProtoFn)
                    .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                    .withReadSessionOptions(
                        ReadSessionOptions.builder()
                            .setSqlFilter(NUM_CHARACTERS_FIELD_NAME + " > 5000")
                            .setSelectedFields(
                                Lists.newArrayList(
                                    NUM_CHARACTERS_FIELD_NAME,
                                    CONTRIBUTOR_USERNAME_FIELD_NAME,
                                    CONTRIBUTOR_IP_FIELD_NAME))
                            .build())
                    .from(DEFAULT_TABLE_REFERENCE));
        break;
      case READ_QUERY_RESULTS_AS_ROW_PROTO:
        editRecords =
            p.apply(
                "ReadQueryResultsAsRowProto",
                BigQueryIO.readViaRowProto(parseRowProtoFn)
                    .withCoder(SerializableCoder.of(TrimmedEditRecord.class))
                    .fromQuery(FILTER_PROJECTION_QUERY_STRING)
                    .usingStandardSql());
        break;
      default:
        throw new IllegalArgumentException("Unknown pipeline type: " + options.getPipelineType());
    }

    // Verify that the expected number of edit records were read.
    PAssert.thatSingleton(editRecords.apply("CountEdits", Count.globally())).isEqualTo(168939084L);

    PCollection<KV<String, Long>> editsPerAuthor =
        editRecords
            .apply(
                "Extract user information (contributor username or IP address) from edit record",
                ParDo.of(new ExtractUserInfoFn()))
            .apply("Count edits per user", Count.perElement());

    // Verify that the expected number of edit authors were found.
    PAssert.thatSingleton("CountEditors", editsPerAuthor.apply(Count.globally()))
        .isEqualTo(16725243L);

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    BigQueryWordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryWordCountOptions.class);
    runBQWordCount(options);
  }
}
