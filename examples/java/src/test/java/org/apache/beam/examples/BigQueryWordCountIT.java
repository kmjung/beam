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
import org.junit.runners.JUnit4;

/** End-to-end integration test of {@link BigQueryWordCount}. */
@RunWith(JUnit4.class)
public class BigQueryWordCountIT {

  private static final PipelineType DEFAULT_PIPELINE_TYPE = PipelineType.READ_AND_FILTER;

  /** Options for the BigQuery word count integration test. */
  public interface BigQueryWordCountITOptions
      extends TestPipelineOptions, BigQueryWordCountOptions {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2EBQWordCount() throws Exception {
    BigQueryWordCountOptions options =
        TestPipeline.testingPipelineOptions().as(BigQueryWordCountOptions.class);
    options.setPipelineType(DEFAULT_PIPELINE_TYPE);
    BigQueryWordCount.runBQWordCount(options);
  }
}
