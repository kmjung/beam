package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final Stream defaultStream = Stream.newBuilder().setName("default stream").build();

  @Test
  public void testGetStartPosition() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition startPosition = tracker.getStartPosition();
    assertEquals(defaultStream, startPosition.getStream());
    assertEquals(100, startPosition.getOffset());
    assertEquals(100, tracker.getStartOffset());
  }

  @Test
  public void testGetStopPosition() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition stopPosition = tracker.getStopPosition();
    assertEquals(defaultStream, stopPosition.getStream());
    assertEquals(200, stopPosition.getOffset());
    assertEquals(200, tracker.getStopOffset());
  }

  @Test
  public void testTryReturnRecordFailsWithWrongStreamContext() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("new stream"))
            .setOffset(100).build();
    expectedException.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithInvalidStartOffset() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(80).build();
    expectedException.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithRepeatedStreamOffset() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    expectedException.expect(IllegalStateException.class);
    tracker.tryReturnRecordAt(true, recordStart);
  }

  @Test
  public void testTryReturnRecordFailsWithPendingSplit() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, true);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testTryReturnRecordSimpleSparse() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
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
  public void testTryReturnRecordSimpleDense() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
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
  public void testSplitAtOffsetFailsWithUnboundedRange() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition splitPosition =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(180).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
  }

  @Test
  public void testSplitAtOffsetFailsIfUnstarted() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition splitPosition =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(180).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
  }

  @Test
  public void testSplitAtOffsetFailsWithWrongStreamContext() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));

    StreamPosition splitPosition =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("new stream"))
            .setOffset(180).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
  }

  @Test
  public void testSplitAtOffsetFailsWithInvalidOffset() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition splitPosition = StreamPosition.newBuilder(recordStart).setOffset(210).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
  }

  @Test
  public void testSplitAtOffsetFailsWithConsumedOffset() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertFalse(tracker.trySplitAtPosition(recordStart));
  }

  @Test
  public void testSplitAtOffset() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(110).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));

    // Sample positions for which split should fail:
    StreamPosition splitPosition = StreamPosition.newBuilder(recordStart).setOffset(109).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
    splitPosition = splitPosition.toBuilder().setOffset(110).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
    splitPosition = splitPosition.toBuilder().setOffset(200).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));
    splitPosition = splitPosition.toBuilder().setOffset(210).build();
    assertFalse(tracker.trySplitAtPosition(splitPosition));

    // Sample positions for which split should succeed:
    splitPosition = splitPosition.toBuilder().setOffset(111).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitPosition));
    splitPosition = splitPosition.toBuilder().setOffset(150).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitPosition));
    splitPosition = splitPosition.toBuilder().setOffset(199).build();
    assertTrue(tracker.copy().trySplitAtPosition(splitPosition));

    // Split at offset 170 and then at offset 150.
    splitPosition = splitPosition.toBuilder().setOffset(170).build();
    assertTrue(tracker.trySplitAtPosition(splitPosition));
    splitPosition = splitPosition.toBuilder().setOffset(150).build();
    assertTrue(tracker.trySplitAtPosition(splitPosition));

    // We should be able to return records at offsets before the new stop offset.
    recordStart = recordStart.toBuilder().setOffset(135).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    recordStart = recordStart.toBuilder().setOffset(149).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));

    // Attempting to return records past the new stop offset should fail.
    recordStart = recordStart.toBuilder().setOffset(150).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.isDone());
  }

  @Test
  public void testTryMarkPendingSplitFailsWithBoundedRange() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
    StreamPosition currentPosition =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertFalse(tracker.tryMarkSplitPending(currentPosition));
  }

  @Test
  public void testTryMarkPendingSplitFailsIfUnstarted() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition currentPosition =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertFalse(tracker.tryMarkSplitPending(currentPosition));
  }

  @Test
  public void testTryMarkPendingSplitFailsWithWrongStreamContext() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));

    StreamPosition currentPosition =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("new stream"))
            .setOffset(150).build();
    assertFalse(tracker.tryMarkSplitPending(currentPosition));
  }

  @Test
  public void testTryMarkPendingSplitFailsWithWrongCurrentOffset() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    StreamPosition currentPosition = StreamPosition.newBuilder(recordStart).setOffset(149).build();
    assertFalse(tracker.tryMarkSplitPending(currentPosition));
  }

  @Test
  public void testTryMarkPendingSplitFailsWithSplitAlreadyPending() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkSplitPending(recordStart));
    assertFalse(tracker.tryMarkSplitPending(recordStart));
  }

  @Test
  public void testTryMarkPendingSplitSimple() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(150).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkSplitPending(recordStart));
    recordStart = recordStart.toBuilder().setOffset(151).build();
    assertFalse(tracker.tryReturnRecordAt(true, recordStart));
    assertFalse(tracker.isDone());
  }

  @Test
  public void testResolvePendingSplitFailsWithNoSplitPending() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    expectedException.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(defaultStream, 100, Long.MAX_VALUE);
  }

  @Test
  public void testResolvePendingSplitFailsIfUpdatingContextAndRange() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkSplitPending(recordStart));
    expectedException.expect(IllegalStateException.class);
    tracker.resolvePendingSplit(Stream.newBuilder().setName("new stream").build(), 50, 200);
  }

  @Test
  public void testResolvePendingSplitUpdatesStreamContextAndRangeIfUnstarted() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, true);
    tracker.resolvePendingSplit(Stream.newBuilder().setName("new stream").build(), 50, 200);

    StreamPosition startPosition =
        StreamPosition.newBuilder().setStream(Stream.newBuilder().setName("new stream"))
            .setOffset(50).build();
    assertEquals(startPosition, tracker.getStartPosition());
  }

  @Test
  public void testResolvePendingSplitUpdatesStreamContext() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkSplitPending(recordStart));
    Stream newStream = Stream.newBuilder().setName("new stream").build();
    tracker.resolvePendingSplit(newStream, 100, Long.MAX_VALUE);
    recordStart = recordStart.toBuilder().setStream(newStream).setOffset(101).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testResolvePendingSplitUpdatesRange() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertTrue(tracker.tryMarkSplitPending(recordStart));
    tracker.resolvePendingSplit(defaultStream, 100, 200);
    recordStart = recordStart.toBuilder().setOffset(101).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
  }

  @Test
  public void testGetPositionForFractionConsumed() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
    StreamPosition expected =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(3).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.0));
    assertEquals(expected, tracker.getPositionForFractionConsumed(1.0 / 6));
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.333));
    expected = expected.toBuilder().setOffset(4).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.334));
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.666));
    expected = expected.toBuilder().setOffset(5).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.667));
    assertEquals(expected, tracker.getPositionForFractionConsumed(0.999));
    expected = expected.toBuilder().setOffset(6).build();
    assertEquals(expected, tracker.getPositionForFractionConsumed(1.0));
  }

  @Test
  public void testGetCurrentPosition() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(200).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertEquals(recordStart, tracker.getPositionForLastRecordStart());
  }

  @Test
  public void testGetFractionConsumedDense() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 3, 6, false);
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
  }

  @Test
  public void testGetFractionConsumedSparse() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, 200, false);
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
  }

  @Test
  public void testGetOrEstimateFractionConsumed() {
    StreamOffsetRangeTracker tracker =
        new StreamOffsetRangeTracker(defaultStream, 100, Long.MAX_VALUE, false);
    assertEquals(0.0, tracker.getOrEstimateFractionConsumed(Long.MAX_VALUE), 1e-6);
    assertEquals(0.0, tracker.getOrEstimateFractionConsumed(200), 1e-6);

    StreamPosition recordStart =
        StreamPosition.newBuilder().setStream(defaultStream).setOffset(100).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertNull(tracker.getOrEstimateFractionConsumed(Long.MAX_VALUE));
    assertEquals(0.0, tracker.getOrEstimateFractionConsumed(180), 1e-6);

    recordStart = recordStart.toBuilder().setOffset(140).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertNull(tracker.getOrEstimateFractionConsumed(Long.MAX_VALUE));
    assertEquals(0.5, tracker.getOrEstimateFractionConsumed(180), 1e-6);

    recordStart = recordStart.toBuilder().setOffset(185).build();
    assertTrue(tracker.tryReturnRecordAt(true, recordStart));
    assertNull(tracker.getOrEstimateFractionConsumed(Long.MAX_VALUE));
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(180), 1e-6);

    tracker.markDone();
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(Long.MAX_VALUE), 1e-6);
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(180), 1e-6);
    assertEquals(1.0, tracker.getOrEstimateFractionConsumed(200), 1e-6);
  }
}
