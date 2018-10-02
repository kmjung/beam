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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StreamOffsetRangeTracker}. */
@RunWith(JUnit4.class)
public class StreamOffsetRangeTrackerTest {
  @Rule public final ExpectedException expected = ExpectedException.none();

  private static final Stream defaultStream = Stream.newBuilder().setName("defaultStream").build();

  @Test
  public void testGetStartPosition() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition startPosition =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertEquals(startPosition, tracker.getStartPosition());
    assertEquals(100, tracker.getStartOffset());
  }

  @Test
  public void testGetStopPosition() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition stopPosition =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(200).build();
    assertEquals(stopPosition, tracker.getStopPosition());
    assertEquals(200, tracker.getStopOffset());
  }

  @Test
  public void testTryReturnRecordFailsWithWrongStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder()
            .setStream(Stream.newBuilder().setName("newStream").build())
            .setOffset(100)
            .build();
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithInvalidOffset() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(80).build();
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithPreviouslyReturnedOffset() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithPendingSplit() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, true);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testTryReturnRecordSimpleSparse() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(110).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(140).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(183).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(210).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.isDone());
  }

  @Test
  public void testTryReturnRecordSimpleDense() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(3).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(4).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(5).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(6).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.isDone());
  }

  @Test
  public void testSplitAtOffsetFailsWithUnboundedRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsIfUnstarted() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition splitOffset =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsWithWrongStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset =
        StreamPosition.newBuilder()
            .setStream(Stream.newBuilder().setName("newStream").build())
            .setOffset(150)
            .build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsWithInvalidOffset() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(250).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsWithConsumedOffset() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertFalse(tracker.trySplitAtPosition(recordStart));
  }

  @Test
  public void testSplitAtOffsetSimple() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(110).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));

    // Example positions at which split attempts should fail.
    StreamPosition splitOffset =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(109).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(110).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(200).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(220).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));

    // Example positions at which split attempts should succeed.
    splitOffset = splitOffset.toBuilder().setOffset(111).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(150).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(199).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));

    // If we split at offset 170 and then at offset 150...
    splitOffset = splitOffset.toBuilder().setOffset(170).build();
    assertTrue(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(150).build();
    assertTrue(tracker.trySplitAtPosition(splitOffset));

    // ... we should be able to return records up to the new stop offset.
    recordStart = recordStart.toBuilder().setOffset(149).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(150).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.isDone());
  }

  @Test
  public void testTryMarkPendingSplitFailsWithBoundedRange() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertFalse(tracker.tryMarkPendingSplit(recordStart));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfUnstarted() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition currentOffset =
        StreamPosition.newBuilder()
            .setStream(defaultStream)
            .setOffset(StreamOffsetRangeTracker.OFFSET_INFINITY)
            .build();
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfInvalidOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(120).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition currentOffset =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(80).build();
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
    currentOffset = currentOffset.toBuilder().setOffset(100).build();
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
    currentOffset = currentOffset.toBuilder().setOffset(150).build();
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfSplitAlreadyPending() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(120).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    assertFalse(tracker.tryMarkPendingSplit(recordStart));
  }

  @Test
  public void testTryMarkPendingSplitSimple() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(120).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    recordStart = recordStart.toBuilder().setOffset(121).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testTryResolvePendingSplitFailsWithBoundedRange() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    expected.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(defaultStream, 100, 200);
  }

  @Test
  public void testTryResolvePendingSplitFailsIfSplitNotPending() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    expected.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY);
  }

  @Test
  public void testTryResolvePendingSplitFailsIfUpdatingStreamContextAndRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    expected.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(Stream.newBuilder().setName("newStream").build(), 50, 200);
  }

  @Test
  public void testTryResolvePendingSplitUpdatesStreamContextAndRangeIfUnstarted() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, true);
    tracker.resolvePendingSplit(Stream.newBuilder().setName("newStream").build(), 50, 200);
    assertEquals(200, tracker.getStopOffset());
  }

  @Test
  public void testTryResolvePendingSplitUpdatesStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    Stream newStream = Stream.newBuilder().setName("newStream").build();
    tracker.resolvePendingSplit(newStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY);
    recordStart = recordStart.toBuilder().setStream(newStream).setOffset(101).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testTryResolvePendingSplitUpdatesRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    tracker.resolvePendingSplit(defaultStream, 100, 200);
    recordStart = recordStart.toBuilder().setOffset(101).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testGetPositionForFractionConsumedDense() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    // [3, 3) represents from [0, 1/3) fraction of [3, 6).
    StreamPosition expected =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(3).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.0));
    assertEquals(expected, tracker.getPositionForFractionConsumed(1.0 / 6));
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.333));
    // [3, 4) represents from [0, 2/3) fraction of [3, 6).
    expected = expected.toBuilder().setOffset(4).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.334));
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.666));
    // [3, 5) represents from [0, 1) fraction of [3, 6).
    expected = expected.toBuilder().setOffset(5).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.667));
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.999));
    // The entire fraction is consumed for fraction 1.
    expected = expected.toBuilder().setOffset(6).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(1.0));
  }

  @Test
  public void testGetFractionConsumedDense() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(3).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(4).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(1.0 / 3, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(5).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(2.0 / 3, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(6).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(1.0, tracker.getFractionConsumed(), 1e-6);
    assertTrue(tracker.isDone());
  }

  @Test
  public void testGetFractionConsumedSparse() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(110).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(0.1, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(0.5, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(195).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(0.95, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(200).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(1.0, tracker.getFractionConsumed(), 1e-6);
    assertTrue(tracker.isDone());
  }

  @Test
  public void testGetOrEstimateFractionConsumed() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(
            defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, false);
    assertEquals(
        0.0, tracker.getOrEstimateFractionConsumed(StreamOffsetRangeTracker.OFFSET_INFINITY), 1e-6);
    assertEquals(0.0, tracker.getOrEstimateFractionConsumed(200), 1e-6);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(
        0.0, tracker.getOrEstimateFractionConsumed(StreamOffsetRangeTracker.OFFSET_INFINITY), 1e-6);
    assertEquals(0.0, tracker.getOrEstimateFractionConsumed(200), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(140).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(
        0.0, tracker.getOrEstimateFractionConsumed(StreamOffsetRangeTracker.OFFSET_INFINITY), 1e-6);
    assertEquals(0.4, tracker.getOrEstimateFractionConsumed(200), 1e-6);
    assertEquals(0.5, tracker.getOrEstimateFractionConsumed(180), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(185).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(
        0.0, tracker.getOrEstimateFractionConsumed(StreamOffsetRangeTracker.OFFSET_INFINITY), 1e-6);
    assertEquals(0.85, tracker.getOrEstimateFractionConsumed(200), 1e-6);
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(180), 1e-6);
    tracker.markDone();
    assertEquals(
        1.0, tracker.getOrEstimateFractionConsumed(StreamOffsetRangeTracker.OFFSET_INFINITY), 1e-6);
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(200), 1e-6);
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(180), 1e-6);
  }
}
