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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.Streamingbuffer;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1alpha1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.RowOuterClass.Row;
import com.google.cloud.bigquery.v3.RowOuterClass.StructField;
import com.google.cloud.bigquery.v3.RowOuterClass.StructType;
import com.google.cloud.bigquery.v3.RowOuterClass.StructValue;
import com.google.cloud.bigquery.v3.RowOuterClass.Type;
import com.google.cloud.bigquery.v3.RowOuterClass.TypeKind;
import com.google.cloud.bigquery.v3.RowOuterClass.Value;
import com.google.cloud.bigquery.v3.TableReferenceProto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.math.BigInteger;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.ReadSessionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/**
 * Tests for {@link BigQueryIO#readViaRowProto} and related functionality.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageApiReadTest {

  private static final String DEFAULT_TABLE_REFERENCE_STRING =
      "foo.com:project-id:dataset_id.table_id";

  private static final String DEFAULT_PROJECT_ID = "foo.com:project-id";
  private static final String DEFAULT_DATASET_ID = "dataset_id";
  private static final String DEFAULT_TABLE_ID = "table_id";

  private transient PipelineOptions options;
  private transient TemporaryFolder testFolder = new TemporaryFolder();
  private transient TestPipeline pipeline;

  /**
   * We need to set up the temporary folder before creating the TestPipeline. Unfortunately, since
   * rule evaluation order is unspecified, unrelated to field order, and separate from construction,
   * we must manually create this test rule.
   */
  @Rule
  public final transient TestRule folderThenPipelineRule =
      (base, description) -> {
        Statement withPipeline = new Statement() {
          @Override
          public void evaluate() throws Throwable {
            options = TestPipeline.testingPipelineOptions();
            options.as(BigQueryOptions.class).setProject("project-id");
            options.as(BigQueryOptions.class).setTempLocation(
                testFolder.getRoot().getAbsolutePath());
            pipeline = TestPipeline.fromOptions(options);
            pipeline.apply(base, description).evaluate();
          }
        };
        return testFolder.apply(withPipeline, description);
      };

  @Rule
  public final transient ExpectedException thrown = ExpectedException.none();

  private final TableReference defaultTableReference =
      new TableReference()
          .setProjectId(DEFAULT_PROJECT_ID)
          .setDatasetId(DEFAULT_DATASET_ID)
          .setTableId(DEFAULT_TABLE_ID);

  private final TableReferenceProto.TableReference defaultTableReferenceProto =
      TableReferenceProto.TableReference.newBuilder()
          .setProjectId(DEFAULT_PROJECT_ID)
          .setDatasetId(DEFAULT_DATASET_ID)
          .setTableId(DEFAULT_TABLE_ID)
          .build();

  private final StructType defaultStructType =
      StructType.newBuilder()
          .addFields(StructField.newBuilder().setFieldName("name")
              .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_STRING)))
          .addFields(StructField.newBuilder().setFieldName("number")
              .setFieldType(Type.newBuilder().setTypeKind(TypeKind.TYPE_INT64)))
          .build();

  private FakeDatasetService fakeDatasetService;
  private FakeJobService fakeJobService;
  private FakeTableReadService fakeTableReadService;
  private FakeBigQueryServices fakeBigQueryServices;

  @Before
  public void setUpTest() {
    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeJobService = new FakeJobService();
    fakeTableReadService = new FakeTableReadService();
    fakeBigQueryServices = new FakeBigQueryServices()
        .withDatasetService(fakeDatasetService)
        .withJobService(fakeJobService)
        .withTableReadService(fakeTableReadService);
  }

  @Test
  public void testBuildTableSource() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING);
    checkTypedReadTableObject(typedRead, DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, DEFAULT_TABLE_ID);
  }

  @Test
  public void testBuildQuerySource() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .fromQuery("SELECT * FROM my_table");
    checkTypedReadQueryObject(typedRead, "SELECT * FROM my_table");
  }

  @Test
  public void testBuildTableSourceWithoutValidation() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING)
        .withoutValidation();
    checkTypedReadTableObject(typedRead, DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, DEFAULT_TABLE_ID,
        false);
  }

  @Test
  public void testBuildQuerySourceWithoutValidation() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .fromQuery("SELECT * FROM my_table")
        .withoutValidation();
    checkTypedReadQueryObject(typedRead, "SELECT * FROM my_table", false);
  }

  @Test
  public void testBuildTableSourceWithDefaultProject() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_DATASET_ID + "." + DEFAULT_TABLE_ID);
    checkTypedReadTableObject(typedRead, null, DEFAULT_DATASET_ID, DEFAULT_TABLE_ID);
  }

  @Test
  public void testBuildTableSourceWithTableReference() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(defaultTableReference);
    checkTypedReadTableObject(typedRead, DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, DEFAULT_TABLE_ID);
  }

  private void checkTypedReadTableObject(BigQueryIO.TypedRead typedRead,
      String project, String dataset, String table) {
    checkTypedReadTableObject(typedRead, project, dataset, table, true);
  }

  private void checkTypedReadQueryObject(BigQueryIO.TypedRead typedRead, String query) {
    checkTypedReadQueryObject(typedRead, query, true);
  }

  private void checkTypedReadTableObject(BigQueryIO.TypedRead typedRead,
      String projectId, String datasetId, String tableId, boolean validate) {
    assertEquals(projectId, typedRead.getTable().getProjectId());
    assertEquals(datasetId, typedRead.getTable().getDatasetId());
    assertEquals(tableId, typedRead.getTable().getTableId());
    assertNull(typedRead.getQuery());
    assertEquals(validate, typedRead.getValidate());
    assertEquals(Method.READ, typedRead.getMethod());
  }

  private void checkTypedReadQueryObject(
      BigQueryIO.TypedRead typedRead, String query, boolean validate) {
    assertNull(typedRead.getTable());
    assertEquals(query, typedRead.getQuery().get());
    assertEquals(validate, typedRead.getValidate());
  }

  @Test
  public void testBuildTableSourceWithNullParseFn() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "A row proto parseFn is required when using TypedRead.Method.READ");
    pipeline.apply(BigQueryIO.readViaRowProto(null)
        .from(DEFAULT_TABLE_REFERENCE_STRING));
    pipeline.run();
  }

  @Test
  public void testBuildQuerySourceWithNullParseFn() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "A row proto parseFn is required when using TypedRead.Method.READ");
    pipeline.apply(BigQueryIO.readViaRowProto(null)
        .fromQuery("SELECT * FROM my_table"));
    pipeline.run();
  }

  @Test
  public void testBuildTableSourceWithFlattenResults() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Specifies a table with a result flattening preference, which only applies to queries");
    pipeline.apply(BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING)
        .withoutResultFlattening());
    pipeline.run();
  }

  @Test
  public void testBuildTableSourceWithFlattenResultsWithoutValidation() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Specifies a table with a result flattening preference, which only applies to queries");
    pipeline.apply(BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING)
        .withoutResultFlattening()
        .withoutValidation());
    pipeline.run();
  }

  @Test
  public void testBuildTableSourceWithUseLegacySql() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Specifies a table with a SQL dialect preference, which only applies to queries");
    pipeline.apply(BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING)
        .usingStandardSql());
    pipeline.run();
  }

  @Test
  public void testBuildTableSourceWithUseLegacySqlWithoutValidation() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Specifies a table with a SQL dialect preference, which only applies to queries");
    pipeline.apply(BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING)
        .usingStandardSql()
        .withoutValidation());
    pipeline.run();
  }

  @Test
  public void testTableSourceDisplayData() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING);
    DisplayData displayData = DisplayData.from(typedRead);
    assertThat(displayData, hasDisplayItem("table", DEFAULT_TABLE_REFERENCE_STRING));
  }

  @Test
  public void testQuerySourceDisplayData() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .fromQuery("SELECT * FROM my_table");
    DisplayData displayData = DisplayData.from(typedRead);
    assertThat(displayData, hasDisplayItem("query", "SELECT * FROM my_table"));
  }

  @Test
  public void testTypedReadName() {
    BigQueryIO.TypedRead typedRead = BigQueryIO.readViaRowProto(SerializableFunctions.identity())
        .from(DEFAULT_TABLE_REFERENCE_STRING);
    assertEquals("BigQueryIO.TypedRead", typedRead.getName());
  }

  @Test
  public void testTableSourceEstimatedSize() throws Exception {
    runTestTableSourceEstimatedSize(false);
  }

  @Test
  public void testTableSourceEstimatedSizeIgnoreStreamingBuffer() throws Exception {
    runTestTableSourceEstimatedSize(true);
  }

  private void runTestTableSourceEstimatedSize(boolean useStreamingBuffer) throws Exception {
    Table table = new Table().setTableReference(defaultTableReference).setNumBytes(100L);
    if (useStreamingBuffer) {
      table.setStreamingBuffer(new Streamingbuffer().setEstimatedBytes(BigInteger.TEN));
    }

    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);
    fakeDatasetService.createTable(table);

    BoundedSource<Row> source = BigQueryStorageTableSource.create(
        ValueProvider.StaticValueProvider.of(defaultTableReference),
        SchemaAndRowProto::getRow,
        ProtoCoder.of(Row.class),
        fakeBigQueryServices,
        null);

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    assertEquals(100, source.getEstimatedSizeBytes(pipelineOptions));
  }

  @Test
  public void testTableSourceEstimatedSizeWithNullTable() throws Exception {
    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);

    BoundedSource<Row> source = BigQueryStorageTableSource.create(
        ValueProvider.StaticValueProvider.of(defaultTableReference),
        SchemaAndRowProto::getRow,
        ProtoCoder.of(Row.class),
        fakeBigQueryServices,
        null);

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    assertEquals(0, source.getEstimatedSizeBytes(pipelineOptions));
  }

  @Test
  public void testTableSourceEstimatedSizeWithDefaultProject() throws Exception {
    Table table = new Table().setTableReference(defaultTableReference).setNumBytes(100L);
    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);
    fakeDatasetService.createTable(table);

    TableReference tableReference =
        new TableReference().setDatasetId(DEFAULT_DATASET_ID).setTableId(DEFAULT_TABLE_ID);
    BoundedSource<Row> source = BigQueryStorageTableSource.create(
        ValueProvider.StaticValueProvider.of(tableReference),
        SchemaAndRowProto::getRow,
        ProtoCoder.of(Row.class),
        fakeBigQueryServices,
        null);

    BigQueryOptions options = PipelineOptionsFactory.fromArgs("--project=" + DEFAULT_PROJECT_ID)
        .as(BigQueryOptions.class);
    assertEquals(100, source.getEstimatedSizeBytes(options));
  }

  private static final long TABLE_SIZE_BYTES = 1024L * 1024L;

  @Test
  public void testTableSourceInitialSplit() throws Exception {
    runTestTableSourceInitialSplit(1024L, 1024);
  }

  @Test
  public void testTableSourceInitialSplitMaxStreamCount() throws Exception {
    runTestTableSourceInitialSplit(16L, 10000);
  }

  @Test
  public void testTableSourceInitialSplitMinStreamCount() throws Exception {
    runTestTableSourceInitialSplit(TABLE_SIZE_BYTES, 10);
  }

  @Test
  public void testTableSourceInitialSplitBundleSizeZero() throws Exception {
    runTestTableSourceInitialSplit(0L, 10);
  }

  private void runTestTableSourceInitialSplit(
      long desiredBundleSizeBytes, int expectedStreamCount) throws Exception {
    Table table = new Table().setTableReference(defaultTableReference)
        .setNumBytes(TABLE_SIZE_BYTES);
    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest createSessionRequest = CreateReadSessionRequest.newBuilder()
        .setTableReference(defaultTableReferenceProto)
        .setRequestedStreams(expectedStreamCount)
        .build();

    ReadSession.Builder sessionBuilder = ReadSession.newBuilder().setName("session");
    for (int i = 0; i < 50; i++) {
      sessionBuilder.addStreams(Stream.newBuilder());
    }

    fakeTableReadService.setCreateSessionResult(createSessionRequest, sessionBuilder.build());

    BoundedSource<Row> source = BigQueryStorageTableSource.create(
        ValueProvider.StaticValueProvider.of(defaultTableReference),
        SchemaAndRowProto::getRow,
        ProtoCoder.of(Row.class),
        fakeBigQueryServices,
        null);

    List<? extends BoundedSource<Row>> sources = source.split(desiredBundleSizeBytes, options);
    assertEquals(50, sources.size());
    long expectedSizeBytes = TABLE_SIZE_BYTES / 50;
    assertEquals(expectedSizeBytes, sources.get(0).getEstimatedSizeBytes(options));
  }

  @Test
  public void testTableSourceInitialSplitWithDefaultProject() throws Exception {
    Table table = new Table().setTableReference(defaultTableReference)
        .setNumBytes(TABLE_SIZE_BYTES);
    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest createReadSessionRequest = CreateReadSessionRequest.newBuilder()
        .setTableReference(defaultTableReferenceProto)
        .setRequestedStreams(1024)
        .build();

    ReadSession readSession = ReadSession.newBuilder()
        .setName("session")
        .addStreams(Stream.newBuilder())
        .build();

    fakeTableReadService.setCreateSessionResult(createReadSessionRequest, readSession);

    TableReference tableReference =
        new TableReference().setDatasetId(DEFAULT_DATASET_ID).setTableId(DEFAULT_TABLE_ID);
    BoundedSource<Row> source = BigQueryStorageTableSource.create(
        ValueProvider.StaticValueProvider.of(tableReference),
        SchemaAndRowProto::getRow,
        ProtoCoder.of(Row.class),
        fakeBigQueryServices,
        null);

    BigQueryOptions options = PipelineOptionsFactory.fromArgs("--project=" + DEFAULT_PROJECT_ID)
        .as(BigQueryOptions.class);
    List<? extends BoundedSource<Row>> sources = source.split(1024, options);
    assertEquals(1, sources.size());
    assertEquals(TABLE_SIZE_BYTES, sources.get(0).getEstimatedSizeBytes(options));
  }

  @Test
  public void testTableSourceInitialSplitOverEmptyTable() throws Exception {
    Table table = new Table().setTableReference(defaultTableReference).setNumBytes(0L);
    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);
    fakeDatasetService.createTable(table);

    CreateReadSessionRequest createReadSessionRequest = CreateReadSessionRequest.newBuilder()
        .setTableReference(defaultTableReferenceProto)
        .setRequestedStreams(10)
        .build();

    // Return a session with no read locations.
    ReadSession readSession = ReadSession.newBuilder().setName("session").build();
    fakeTableReadService.setCreateSessionResult(createReadSessionRequest, readSession);

    BoundedSource<Row> source = BigQueryStorageTableSource.create(
        ValueProvider.StaticValueProvider.of(defaultTableReference),
        SchemaAndRowProto::getRow,
        ProtoCoder.of(Row.class),
        fakeBigQueryServices,
        null);

    List<? extends BoundedSource<Row>> sources = source.split(1024, options);
    assertEquals(0, sources.size());
  }

  @Test
  public void testCoderInference() {
    // Lambdas erase too much type information -- use an anonymous class here.
    SerializableFunction<SchemaAndRowProto, Row> parseFn =
        new SerializableFunction<SchemaAndRowProto, Row>() {
          @Override
          public Row apply(SchemaAndRowProto input) {
            return input.getRow();
          }
        };

    assertEquals(ProtoCoder.of(Row.class),
        BigQueryIO.readViaRowProto(parseFn).inferCoder(CoderRegistry.createDefault()));
  }

  private final CreateReadSessionRequest defaultCreateReadSessionRequest =
      CreateReadSessionRequest.newBuilder()
          .setTableReference(TableReferenceProto.TableReference.newBuilder()
              .setProjectId(DEFAULT_PROJECT_ID)
              .setDatasetId(DEFAULT_DATASET_ID)
              .setTableId(DEFAULT_TABLE_ID))
          .setRequestedStreams(10)
          .build();

  private final ReadRowsRequest defaultReadRowsRequest =
      ReadRowsRequest.newBuilder()
          .setReadPosition(StreamPosition.newBuilder().setStream(
              Stream.newBuilder().setName("stream name")))
          .build();

  @Test
  public void testReadFromTableSource() throws Exception {
    runTestReadFromTableSource(null, defaultCreateReadSessionRequest, defaultReadRowsRequest);
  }

  @Test
  public void testReadFromTableSourceWithFilter() throws Exception {

    ReadSessionOptions readSessionOptions = ReadSessionOptions.builder()
        .setSqlFilter("SQL filter")
        .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder().setFilter("SQL filter"))
            .build();

    runTestReadFromTableSource(readSessionOptions, createReadSessionRequest,
        defaultReadRowsRequest);
  }

  @Test
  public void testReadFromTableSourceWithSelectedFields() throws Exception {

    ReadSessionOptions readSessionOptions = ReadSessionOptions.builder()
        .setSelectedFields(Lists.newArrayList("field 1", "field 2"))
        .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder()
                .addSelectedFields("field 1")
                .addSelectedFields("field 2"))
            .build();

    runTestReadFromTableSource(readSessionOptions, createReadSessionRequest,
        defaultReadRowsRequest);
  }

  @Test
  public void testReadFromTableSourceWithFilterAndSelectedFields() throws Exception {

    ReadSessionOptions readSessionOptions = ReadSessionOptions.builder()
        .setSqlFilter("SQL filter")
        .setSelectedFields(Lists.newArrayList("field 1", "field 2"))
        .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder()
                .setFilter("SQL filter")
                .addSelectedFields("field 1")
                .addSelectedFields("field 2"))
            .build();

    runTestReadFromTableSource(readSessionOptions, createReadSessionRequest,
        defaultReadRowsRequest);
  }

  @Test
  public void testReadFromTableSourceWithAllReadOptions() throws Exception {

    ReadSessionOptions readSessionOptions = ReadSessionOptions.builder()
        .setSqlFilter("SQL filter")
        .setSelectedFields(Lists.newArrayList("field 1", "field 2"))
        .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder()
                .setFilter("SQL filter")
                .addSelectedFields("field 1")
                .addSelectedFields("field 2"))
            .build();

    ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder(defaultReadRowsRequest)
        .build();

    runTestReadFromTableSource(readSessionOptions, createReadSessionRequest, readRowsRequest);
  }

  private void runTestReadFromTableSource(
      ReadSessionOptions readSessionOptions,
      CreateReadSessionRequest createReadSessionRequest,
      ReadRowsRequest readRowsRequest)
      throws Exception {

    ReadSession readSession = ReadSession.newBuilder()
        .setName("session name")
        .setProjectedSchema(defaultStructType)
        .addStreams(Stream.newBuilder().setName("stream name"))
        .build();

    fakeTableReadService.setCreateSessionResult(createReadSessionRequest, readSession);

    List<ReadRowsResponse> readRowsResponses = Lists.newArrayList(
        ReadRowsResponse.newBuilder()
            .addRows(Row.newBuilder().setValue(StructValue.newBuilder()
                .addFields(Value.newBuilder().setStringValue("a"))
                .addFields(Value.newBuilder().setInt64Value(1L))))
            .addRows(Row.newBuilder().setValue(StructValue.newBuilder()
                .addFields(Value.newBuilder().setStringValue("b"))
                .addFields(Value.newBuilder().setInt64Value(2L))))
            .build(),
        ReadRowsResponse.newBuilder()
            .addRows(Row.newBuilder().setValue(StructValue.newBuilder()
                .addFields(Value.newBuilder().setStringValue("c"))
                .addFields(Value.newBuilder().setInt64Value(3L))))
            .build());

    fakeTableReadService.setReadRowsResponses(readRowsRequest, readRowsResponses);

    Table table = new Table().setTableReference(defaultTableReference).setNumBytes(1L);
    fakeDatasetService.createDataset(DEFAULT_PROJECT_ID, DEFAULT_DATASET_ID, "", "", null);
    fakeDatasetService.createTable(table);

    BigQueryOptions options = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    options.setProject(DEFAULT_PROJECT_ID);
    Pipeline pipeline = TestPipeline.create(options);

    PCollection<KV<String, Long>> output = pipeline.apply(
        BigQueryIO.readViaRowProto(
            (input) -> KV.of((String) input.get("name"), (Long) input.get("number")))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            .from(DEFAULT_TABLE_REFERENCE_STRING)
            .withTestServices(fakeBigQueryServices)
            .withReadSessionOptions(readSessionOptions));

    PAssert.that(output).containsInAnyOrder(
        ImmutableList.of(KV.of("a", 1L), KV.of("b", 2L), KV.of("c", 3L)));

    pipeline.run();
  }

  @Test
  public void testReadFromQuerySource() throws Exception {
    runTestReadFromQuerySource(null, defaultCreateReadSessionRequest, defaultReadRowsRequest);
  }

  @Test
  public void testReadFromQuerySourceWithFilter() throws Exception {

    ReadSessionOptions readSessionOptions =
        ReadSessionOptions.builder().setSqlFilter("SQL filter").build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder().setFilter("SQL filter"))
            .build();

    runTestReadFromQuerySource(readSessionOptions, createReadSessionRequest,
        defaultReadRowsRequest);
  }

  @Test
  public void testReadFromQuerySourceWithSelectedFields() throws Exception {

    ReadSessionOptions readSessionOptions =
        ReadSessionOptions.builder()
            .setSelectedFields(
                Lists.newArrayList("field 1", "field 2"))
            .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder()
                .addSelectedFields("field 1")
                .addSelectedFields("field 2"))
            .build();

    runTestReadFromQuerySource(readSessionOptions, createReadSessionRequest,
        defaultReadRowsRequest);
  }

  @Test
  public void testReadFromQuerySourceWithFilterAndSelectedFields() throws Exception {

    ReadSessionOptions readSessionOptions = ReadSessionOptions.builder()
        .setSqlFilter("SQL filter")
        .setSelectedFields(Lists.newArrayList("field 1", "field 2"))
        .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder()
                .setFilter("SQL filter")
                .addSelectedFields("field 1")
                .addSelectedFields("field 2"))
            .build();

    runTestReadFromQuerySource(readSessionOptions, createReadSessionRequest,
        defaultReadRowsRequest);
  }

  @Test
  public void testReadFromQuerySourceWithAllReadOptions() throws Exception {

    ReadSessionOptions readSessionOptions = ReadSessionOptions.builder()
        .setSqlFilter("SQL filter")
        .setSelectedFields(Lists.newArrayList("field 1", "field 2"))
        .build();

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder(defaultCreateReadSessionRequest)
            .setReadOptions(TableReadOptions.newBuilder()
                .setFilter("SQL filter")
                .addSelectedFields("field 1")
                .addSelectedFields("field 2"))
            .build();

    ReadRowsRequest readRowsRequest =
        ReadRowsRequest.newBuilder(defaultReadRowsRequest)
            .build();

    runTestReadFromQuerySource(readSessionOptions, createReadSessionRequest, readRowsRequest);
  }

  private void runTestReadFromQuerySource(
      ReadSessionOptions readSessionOptions,
      CreateReadSessionRequest createReadSessionRequest,
      ReadRowsRequest readRowsRequest)
      throws Exception {

    BigQueryOptions bqOptions =
        PipelineOptionsFactory.fromArgs("--project=" + DEFAULT_PROJECT_ID)
            .as(BigQueryOptions.class);

    String stepUuid = "testStepUuid";

    TableReference tempTableReference = createTempTableReference(
        DEFAULT_PROJECT_ID, createJobIdToken(bqOptions.getJobName(), stepUuid));

    fakeDatasetService.createDataset(
        tempTableReference.getProjectId(),
        tempTableReference.getDatasetId(),
        "location",
        "description",
        null);

    fakeDatasetService.createTable(new Table()
        .setTableReference(tempTableReference)
        .setSchema(new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")))));

    List<TableRow> expected = ImmutableList.of(
        new TableRow().set("name", "a").set("number", 1L),
        new TableRow().set("name", "b").set("number", 2L),
        new TableRow().set("name", "c").set("number", 3L));

    String encodedQuery = FakeBigQueryServices.encodeQuery(expected);

    fakeJobService.expectDryRunQuery(
        DEFAULT_PROJECT_ID,
        encodedQuery,
        new JobStatistics().setQuery(
            new JobStatistics2()
                .setTotalBytesProcessed(TABLE_SIZE_BYTES)
                .setReferencedTables(ImmutableList.of(tempTableReference))));

    CreateReadSessionRequest expectedCreateReadSessionRequest =
        CreateReadSessionRequest.newBuilder(createReadSessionRequest)
            .setTableReference(TableReferenceProto.TableReference.newBuilder()
                .setProjectId(tempTableReference.getProjectId()))
            .build();

    ReadSession readSession = ReadSession.newBuilder()
        .setName("session name")
        .setProjectedSchema(defaultStructType)
        .addStreams(Stream.newBuilder().setName("stream name"))
        .build();

    fakeTableReadService.setCreateSessionResult(expectedCreateReadSessionRequest, readSession);

    List<ReadRowsResponse> readRowsResponses = Lists.newArrayList(
        ReadRowsResponse.newBuilder()
            .addRows(Row.newBuilder().setValue(StructValue.newBuilder()
                .addFields(Value.newBuilder().setStringValue("a"))
                .addFields(Value.newBuilder().setInt64Value(1L))))
            .addRows(Row.newBuilder().setValue(StructValue.newBuilder()
                .addFields(Value.newBuilder().setStringValue("b"))
                .addFields(Value.newBuilder().setInt64Value(2L))))
            .build(),
        ReadRowsResponse.newBuilder()
            .addRows(Row.newBuilder().setValue(StructValue.newBuilder()
                .addFields(Value.newBuilder().setStringValue("c"))
                .addFields(Value.newBuilder().setInt64Value(3L))))
            .build());

    fakeTableReadService.setReadRowsResponses(readRowsRequest, readRowsResponses);

    Pipeline pipeline = TestPipeline.create(bqOptions);

    PCollection<KV<String, Long>> output = pipeline.apply(
        BigQueryIO.readViaRowProto(
            (input) -> KV.of((String) input.get("name"), (Long) input.get("number")))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            .fromQuery(encodedQuery)
            .withTestServices(fakeBigQueryServices)
            .withReadSessionOptions(readSessionOptions));

    PAssert.that(output).containsInAnyOrder(
        ImmutableList.of(KV.of("a", 1L), KV.of("b", 2L), KV.of("c", 3L)));

    pipeline.run();
  }
}
