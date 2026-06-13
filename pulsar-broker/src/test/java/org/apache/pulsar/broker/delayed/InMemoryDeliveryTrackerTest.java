/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.delayed;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InMemoryDeliveryTrackerTest extends AbstractDeliveryTrackerTest {

    @DataProvider(name = "delayedTracker")
    public Object[][] provider(Method method) throws Exception {
        dispatcher = mock(AbstractPersistentDispatcherMultipleConsumers.class);
        clock = mock(Clock.class);
        clockTime = new AtomicLong();
        when(clock.millis()).then(x -> clockTime.get());

        final String methodName = method.getName();
        return switch (methodName) {
            case "test" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            false, 0)
            }};
            case "testWithTimer" -> {
                Timer timer = mock(Timer.class);

                AtomicLong clockTime = new AtomicLong();
                Clock clock = mock(Clock.class);
                when(clock.millis()).then(x -> clockTime.get());

                NavigableMap<Long, TimerTask> tasks = new TreeMap<>();

                when(timer.newTimeout(any(), anyLong(), any())).then(invocation -> {
                    TimerTask task = invocation.getArgument(0, TimerTask.class);
                    long timeout = invocation.getArgument(1, Long.class);
                    TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
                    long scheduleAt = clockTime.get() + unit.toMillis(timeout);
                    tasks.put(scheduleAt, task);

                    Timeout t = mock(Timeout.class);
                    when(t.cancel()).then(i -> {
                        return tasks.remove(scheduleAt, task);
                    });
                    return t;
                });

                yield new Object[][]{{
                        new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                                false, 0),
                        tasks
                }};
            }
            case "testAddWithinTickTime" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 100, clock,
                            false, 0)
            }};
            case "testAddMessageWithStrictDelay" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowBeforeTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1000, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterNowAfterTickTimeFrequencyWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
            case "testAddMessageWithDeliverAtTimeAfterFullTickTimeWithStrict" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 500, clock,
                            true, 0)
            }};
            case "testWithFixedDelays", "testWithMixedDelays", "testWithNoDelays" -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 8, clock,
                            true, 100)
            }};
            case "testStrictModeNeverDeliversEarlyAndKeepsTimerArmed",
                 "testStaleTimerTriggerDoesNotClearNewerTimer" -> {
                Timer mockTimer = mock(Timer.class);
                NavigableMap<Long, Map.Entry<TimerTask, Timeout>> tasks = new TreeMap<>();
                when(mockTimer.newTimeout(any(), anyLong(), any())).then(invocation -> {
                    TimerTask task = invocation.getArgument(0, TimerTask.class);
                    long timeout = invocation.getArgument(1, Long.class);
                    TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
                    long scheduleAt = clockTime.get() + unit.toMillis(timeout);
                    Timeout t = mock(Timeout.class);
                    Map.Entry<TimerTask, Timeout> entry = Map.entry(task, t);
                    AtomicBoolean cancelled = new AtomicBoolean();
                    when(t.cancel()).then(i -> {
                        cancelled.set(true);
                        return tasks.remove(scheduleAt, entry);
                    });
                    when(t.isCancelled()).then(i -> cancelled.get());
                    tasks.put(scheduleAt, entry);
                    return t;
                });
                yield new Object[][]{{
                        new InMemoryDelayedDeliveryTracker(dispatcher, mockTimer, 1000, clock,
                                true, 0),
                        tasks,
                        mockTimer
                }};
            }
            default -> new Object[][]{{
                    new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                            true, 0)
            }};
        };
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithFixedDelays(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), 5);
        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= tracker.getFixedDelayDetectionLookahead(); i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        clockTime.set(tracker.getFixedDelayDetectionLookahead() * 10);

        tracker.getScheduledMessages(100);

        assertFalse(tracker.shouldPauseAllDeliveries());

        // Empty the tracker
        int removed = 0;
        do {
            removed = tracker.getScheduledMessages(100).size();
        } while (removed > 0);

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithMixedDelays(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= tracker.getFixedDelayDetectionLookahead(); i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        // Add message with earlier delivery time
        assertTrue(tracker.addMessage(5, 6, 5));

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testWithNoDelays(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        assertTrue(tracker.addMessage(1, 1, 10));
        assertTrue(tracker.addMessage(2, 2, 20));
        assertTrue(tracker.addMessage(3, 3, 30));
        assertTrue(tracker.addMessage(4, 4, 40));
        assertTrue(tracker.addMessage(5, 5, 50));

        assertFalse(tracker.shouldPauseAllDeliveries());

        for (int i = 6; i <= tracker.getFixedDelayDetectionLookahead(); i++) {
            assertTrue(tracker.addMessage(i, i, i * 10));
        }

        assertTrue(tracker.shouldPauseAllDeliveries());

        // Add message with no-delay
        assertFalse(tracker.addMessage(5, 6, -1L));

        assertFalse(tracker.shouldPauseAllDeliveries());

        tracker.close();
    }

    @Test
    public void testClose() throws Exception {
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-in-memory-delayed-delivery-test"),
                1, TimeUnit.MILLISECONDS);

        AbstractPersistentDispatcherMultipleConsumers dispatcher =
                mock(AbstractPersistentDispatcherMultipleConsumers.class);

        AtomicLong clockTime = new AtomicLong();
        Clock clock = mock(Clock.class);
        when(clock.millis()).then(x -> clockTime.get());

        final Exception[] exceptions = new Exception[1];

        InMemoryDelayedDeliveryTracker tracker = new InMemoryDelayedDeliveryTracker(dispatcher, timer, 1, clock,
                true, 0) {
            @Override
            public void run(Timeout timeout) throws Exception {
                rescheduleTimer(1);
                if (timeout == null || timeout.isCancelled()) {
                    return;
                }
                try {
                    this.delayedMessageMap.firstKey();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptions[0] = e;
                }
            }
        };

        tracker.addMessage(1, 1, 10);
        clockTime.set(10);

        Thread.sleep(300);

        tracker.close();

        assertNull(exceptions[0]);
    }

    @Test(dataProvider = "delayedTracker")
    public void testStrictModeNeverDeliversEarlyAndKeepsTimerArmed(InMemoryDelayedDeliveryTracker tracker,
            NavigableMap<Long, Map.Entry<TimerTask, Timeout>> tasks, Timer mockTimer) throws Exception {
        clockTime.set(0);

        assertTrue(tracker.addMessage(1, 1, 60400));
        assertTrue(tracker.addMessage(2, 2, 61000));
        assertEquals(tasks.size(), 1, "a delivery timer should be armed for the earliest message");
        assertEquals(tasks.firstKey().longValue(), 60416, "the timer should target M1's rounded-up bucket");

        clockTime.set(60000);
        assertFalse(tracker.hasMessageAvailable());
        assertTrue(tracker.getScheduledMessages(100).isEmpty(),
                "strict mode must not deliver a message before its deliverAt time");
        assertEquals(tasks.size(), 1, "the delivery timer must remain armed");

        clockTime.set(60416);
        Map.Entry<TimerTask, Timeout> firedTimeout = tasks.pollFirstEntry().getValue();
        firedTimeout.getKey().run(firedTimeout.getValue());
        Set<Position> scheduled = tracker.getScheduledMessages(100);
        assertEquals(scheduled, Set.of(PositionFactory.create(1, 1)));

        assertEquals(tracker.getNumberOfDelayedMessages(), 1);
        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tasks.size(), 1, "a delivery timer must remain armed for the pending message M2");
        assertEquals(tasks.firstKey().longValue(), 61440, "the timer should target M2's rounded-up bucket");

        clockTime.set(61440);
        firedTimeout = tasks.pollFirstEntry().getValue();
        firedTimeout.getKey().run(firedTimeout.getValue());
        scheduled = tracker.getScheduledMessages(100);
        assertEquals(scheduled, Set.of(PositionFactory.create(2, 2)));
        assertEquals(tracker.getNumberOfDelayedMessages(), 0);

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testStaleTimerTriggerDoesNotClearNewerTimer(InMemoryDelayedDeliveryTracker tracker,
            NavigableMap<Long, Map.Entry<TimerTask, Timeout>> tasks, Timer mockTimer) throws Exception {
        clockTime.set(0);

        assertTrue(tracker.addMessage(2, 2, 61000));
        assertEquals(tasks.firstKey().longValue(), 61440);
        assertTrue(tracker.addMessage(1, 1, 60400));
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.firstKey().longValue(), 60416, "M1's timer should have replaced M2's");

        Timeout staleTimeout = mock(Timeout.class);
        tracker.run(staleTimeout);

        assertFalse(tracker.hasMessageAvailable());
        assertEquals(tasks.size(), 1);
        assertEquals(tasks.firstKey().longValue(), 60416);
        verify(mockTimer, times(2)).newTimeout(any(), anyLong(), any());

        clockTime.set(60416);
        Map.Entry<TimerTask, Timeout> firedTimeout = tasks.pollFirstEntry().getValue();
        firedTimeout.getKey().run(firedTimeout.getValue());
        Set<Position> scheduled = tracker.getScheduledMessages(100);
        assertEquals(scheduled, Set.of(PositionFactory.create(1, 1)));
        assertEquals(tasks.size(), 1, "a delivery timer must be re-armed for the still pending M2");

        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testDelaySequence(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        assertFalse(tracker.hasMessageAvailable());

        int messageCount = 5;
        for (int i = 1; i <= messageCount; i++) {
            assertTrue(tracker.addMessage(i, i, 1));
        }
        clockTime.set(10);
        assertTrue(tracker.hasMessageAvailable());
        assertEquals(tracker.getNumberOfDelayedMessages(), messageCount);

        for (int i = 1; i <= messageCount; i++) {
            Set<Position> scheduled = tracker.getScheduledMessages(1);
            assertEquals(scheduled.size(), 1);
            Position position = scheduled.iterator().next();
            assertEquals(position.getLedgerId(), i);
            assertEquals(position.getEntryId(), i);
        }
        tracker.close();
    }

    @Test(dataProvider = "delayedTracker")
    public void testAddMultipleMessagesSameWindow(InMemoryDelayedDeliveryTracker tracker) throws Exception {
        tracker.addMessage(1, 1, 50);
        tracker.addMessage(1, 1, 50);
        tracker.addMessage(1, 1, 50);

        clockTime.set(60);

        tracker.getScheduledMessages(10);
    }

    @Test(dataProvider = "delayedTracker")
    public void testGetScheduledMessagesWithMaxMessagesSmallerThanBucket(InMemoryDelayedDeliveryTracker tracker)
            throws Exception {
        clockTime.set(0);

        assertTrue(tracker.addMessage(1, 0, 10));
        assertTrue(tracker.addMessage(1, 1, 10));
        for (int entryId = 0; entryId < 5; entryId++) {
            assertTrue(tracker.addMessage(2, entryId, 10));
        }
        assertEquals(tracker.getNumberOfDelayedMessages(), 7);

        clockTime.set(10);

        Set<Position> scheduled = tracker.getScheduledMessages(4);
        assertEquals(scheduled, Set.of(
                PositionFactory.create(1, 0),
                PositionFactory.create(1, 1),
                PositionFactory.create(2, 0),
                PositionFactory.create(2, 1)));
        assertEquals(tracker.getNumberOfDelayedMessages(), 3);

        scheduled = tracker.getScheduledMessages(2);
        assertEquals(scheduled, Set.of(
                PositionFactory.create(2, 2),
                PositionFactory.create(2, 3)));
        assertEquals(tracker.getNumberOfDelayedMessages(), 1);

        scheduled = tracker.getScheduledMessages(10);
        assertEquals(scheduled, Set.of(PositionFactory.create(2, 4)));
        assertEquals(tracker.getNumberOfDelayedMessages(), 0);
        assertFalse(tracker.hasMessageAvailable());

        tracker.close();
    }
}
