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

/**
 * Tests for {@link StreamOffsetRangeTracker}.
 */
@RunWith(JUnit4.class)
public class StreamOffsetRangeTrackerTest {
  @Rule public final ExpectedException expected = ExpectedException.none();

  private static final Stream defaultStream = Stream.newBuilder().setName("defaultStream").build();

  @Test
  public void testGetStartPosition() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition startPosition = defaultStreamPosition(100);
    assertEquals(startPosition, tracker.getStartPosition());
  }

  @Test
  public void testGetStopPosition() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition stopPosition = defaultStreamPosition(200);
    assertEquals(stopPosition, tracker.getStopPosition());
  }

  @Test
  public void testTryReturnRecordFailsIfNotAtSplitPoint() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    expected.expect(IllegalArgumentException.class);
    tracker.tryReturnRecordAt(false, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithWrongStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("newStream"))
            .setOffset(100).build();
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithInvalidOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(80);
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithPreviouslyReturnedOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    expected.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithPendingSplit() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, true);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertFalse(tracker.isDone());
  }

  @Test
  public void testTryReturnRecordSimpleSparse() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(110);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(140).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(183).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(210).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testTryReturnRecordSimpleDense() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    StreamPosition recordStart = defaultStreamPosition(3);
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
  public void testTryReturnRecordFailsIfSplitPending() throws Exception {
    StreamOffsetRangeTracker tracker = new StreamOffsetRangeTracker(
        defaultStream, 100, StreamOffsetRangeTracker.OFFSET_INFINITY, true);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testSplitAtOffsetFailsWithUnboundedRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset = StreamPosition.newBuilder(recordStart).setOffset(150).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsIfUnstarted() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition splitOffset = defaultStreamPosition(150);
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsWithWrongStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("newStream"))
            .setOffset(150).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsWithInvalidOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset = StreamPosition.newBuilder(recordStart).setOffset(80).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(200).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffsetFailsWithConsumedOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(101).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitOffset = StreamPosition.newBuilder(recordStart).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
  }

  @Test
  public void testSplitAtOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart = defaultStreamPosition(110);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));

    // Example positions we shouldn't split at, when last record is [110, 200):
    StreamPosition splitOffset = StreamPosition.newBuilder(recordStart).setOffset(109).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(110).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(200).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(210).build();
    assertFalse(tracker.trySplitAtPosition(splitOffset));

    // Example positions we *should* split at:
    splitOffset = splitOffset.toBuilder().setOffset(111).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(129).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(130).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(131).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(150).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(199).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitOffset));

    // If we split at 170 and then at 150:
    splitOffset = splitOffset.toBuilder().setOffset(170).build();
    assertTrue(tracker.trySplitAtPosition(splitOffset));
    splitOffset = splitOffset.toBuilder().setOffset(150).build();
    assertTrue(tracker.trySplitAtPosition(splitOffset));

    // We should be able to return a record starting before the new stop offset.
    recordStart = recordStart.toBuilder().setOffset(149).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(150).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.isDone());
  }

  @Test
  public void testTryMarkPendingSplitFailsWithBoundedRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordOffset = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordOffset));
    assertFalse(tracker.tryMarkPendingSplit(recordOffset));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfUnstarted() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition currentOffset = defaultStreamPosition(100);
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfInWrongStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition currentOffset =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("newStream"))
            .setOffset(100).build();
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfInvalidOffset() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition currentOffset = defaultStreamPosition(80);
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
    currentOffset = currentOffset.toBuilder().setOffset(105).build();
    assertFalse(tracker.tryMarkPendingSplit(currentOffset));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfSplitAlreadyPending() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    // The second call should fail since a split is already pending.
    assertFalse(tracker.tryMarkPendingSplit(recordStart));
  }

  @Test
  public void testTryMarkPendingSplitSimple() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(110);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    recordStart = recordStart.toBuilder().setOffset(120).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertFalse(tracker.isDone());
  }

  @Test
  public void testResolvePendingSplitFailsWithBoundedRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, true);
    expected.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(defaultStream, 100, 150);
  }

  @Test
  public void testResolvePendingSplitFailsIfSplitNotPending() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    expected.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(defaultStream, 100, Long.MAX_VALUE);
  }

  @Test
  public void testResolvePendingSplitFailsIfUpdatingStreamContextAndRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    expected.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(Stream.newBuilder().setName("newStream").build(), 50, 200);
  }

  @Test
  public void testResolvePendingSplitUpdatesStreamContextAndRangeIfUnstarted() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, true);
    tracker.resolvePendingSplit(Stream.newBuilder().setName("newStream").build(), 50, 200);
  }

  @Test
  public void testResolvePendingSplitUpdatesStreamContext() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    Stream newStream = Stream.newBuilder().setName("newStream").build();
    tracker.resolvePendingSplit(newStream, 100, Long.MAX_VALUE);
    recordStart = recordStart.toBuilder().setStream(newStream).setOffset(101).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testResolvePendingSplitUpdatesRange() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkPendingSplit(recordStart));
    tracker.resolvePendingSplit(defaultStream, 100, 300);
    recordStart = recordStart.toBuilder().setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testGetPositionForFractionDense() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    // [3, 3) represents from [0, 1/3) fraction of [3, 6)
    assertEquals(3, tracker.getPositionForFractionConsumed(0.0));
    assertEquals(3, tracker.getPositionForFractionConsumed(1.0 / 6));
    assertEquals(3, tracker.getPositionForFractionConsumed(0.333));
    // [3, 4) represents from [0, 2/3) fraction of [3, 6)
    assertEquals(4, tracker.getPositionForFractionConsumed(0.334));
    assertEquals(4, tracker.getPositionForFractionConsumed(0.666));
    // [3, 5) represents from [0, 1) fraction of [3, 6)
    assertEquals(5, tracker.getPositionForFractionConsumed(0.667));
    assertEquals(5, tracker.getPositionForFractionConsumed(0.999));
    // The whole [3, 6) range is consumed for fraction 1.
    assertEquals(6, tracker.getPositionForFractionConsumed(1.0));
  }

  @Test
  public void testGetFractionConsumedDense() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    StreamPosition recordStart = defaultStreamPosition(3);
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
  }

  @Test
  public void testGetFractionConsumedSparse() throws Exception {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    StreamPosition recordStart = defaultStreamPosition(100);
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(0.0, tracker.getFractionConsumed(), 1e-6);
    recordStart = recordStart.toBuilder().setOffset(110).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    // Consumed positions through 109 = total 10 positions of 100.
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
  }

  private StreamPosition defaultStreamPosition(long offset) {
    return StreamPosition.newBuilder().setStream(defaultStream).setOffset(offset).build();
  }
}
