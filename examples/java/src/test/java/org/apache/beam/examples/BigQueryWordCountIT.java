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

import org.apache.beam.examples.BigQueryWordCount.BigQueryWordCountOptions;
import org.apache.beam.examples.BigQueryWordCount.BigQueryWordCountOptions.PipelineType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** End-to-end integration tests of {@link BigQueryWordCount}. */
@RunWith(Parameterized.class)
public class BigQueryWordCountIT {

  /** Options for the BigQuery word count integration test. */
  public interface BigQueryWordCountITOptions
      extends TestPipelineOptions, BigQueryWordCountOptions {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Parameters
  public static Object[] data() {
    return new Object[] {
      PipelineType.EXPORT_TABLE_DATA,
      PipelineType.EXPORT_QUERY_RESULTS,
      PipelineType.READ_TABLE_DATA,
      PipelineType.READ_TABLE_DATA_AND_FILTER,
      PipelineType.READ_QUERY_RESULTS,
      PipelineType.READ_TABLE_DATA_AS_ROW_PROTO,
      PipelineType.READ_TABLE_DATA_AS_ROW_PROTO_AND_FILTER,
      PipelineType.READ_QUERY_RESULTS_AS_ROW_PROTO,
    };
  }

  @Parameter public PipelineType pipelineType;

  @Test
  public void testE2EBQWordCount() throws Exception {
    BigQueryWordCountOptions options =
        TestPipeline.testingPipelineOptions().as(BigQueryWordCountOptions.class);
    options.setPipelineType(pipelineType);
    BigQueryWordCount.runBQWordCount(options);
  }
}
