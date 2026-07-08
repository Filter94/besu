/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.consensus.common.bft.blockcreation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;

import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftProcessor;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.EventMultiplexer;
import org.hyperledger.besu.consensus.common.bft.events.BftEvent;
import org.hyperledger.besu.consensus.common.bft.events.BlockTimerExpiry;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Exercises the merge transition path with a real {@link BftProcessor} running on real {@link
 * BftExecutors}: the TTD watcher in TransitionBesuControllerBuilder calls {@code disable()} then
 * {@code stop()} on the coordinator, and does so synchronously from the BFT event thread itself
 * (QBFT imports its own sealed blocks inline, which fires the block-added observers that drive the
 * merge state callback). After that, no further queued events may be dispatched — each dispatched
 * BlockTimerExpiry would seal another block past TTD.
 */
@ExtendWith(MockitoExtension.class)
public class BftMiningCoordinatorTransitionTest {

  @Mock private BftEventHandler eventHandler;
  @Mock private BftBlockCreatorFactory<?> blockCreatorFactory;
  @Mock private Blockchain blockchain;

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  public void stopFromEventThreadHaltsEventProcessingWithoutDeadlock() throws InterruptedException {
    final BftEventQueue eventQueue = new BftEventQueue(1000);
    eventQueue.start();

    final BftExecutors bftExecutors =
        BftExecutors.create(new NoOpMetricsSystem(), BftExecutors.ConsensusType.QBFT);

    final AtomicInteger handledEvents = new AtomicInteger(0);
    final AtomicReference<BftMiningCoordinator> coordinatorRef = new AtomicReference<>();

    // Simulates exactly what the TTD watcher does when the terminal block is imported:
    // disable() then stop(), invoked on the BFT event thread while it is mid-dispatch.
    final EventMultiplexer eventMultiplexer =
        new EventMultiplexer(eventHandler) {
          @Override
          public void handleBftEvent(final BftEvent bftEvent) {
            if (handledEvents.incrementAndGet() == 1) {
              coordinatorRef.get().disable();
              coordinatorRef.get().stop();
            }
          }
        };

    final BftProcessor bftProcessor = new BftProcessor(eventQueue, eventMultiplexer);
    final BftMiningCoordinator coordinator =
        new BftMiningCoordinator(
            bftExecutors, eventHandler, bftProcessor, blockCreatorFactory, blockchain, eventQueue);
    coordinatorRef.set(coordinator);

    coordinator.enable();
    coordinator.start();

    // Two queued events: the first triggers the merge transition stop; the second must
    // never be dispatched (it would be "the extra QBFT block sealed past TTD").
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));

    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> !coordinator.isMining());

    // Allow time for a wrongly-dispatched second event to surface before asserting.
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(handledEvents.get()).isEqualTo(1));

    // stop() offloads its blocking teardown to a separate thread when invoked from the event
    // thread; wait for that teardown to finish so the BftProcessorExecutor/BftTimerExecutor
    // threads it shuts down don't leak into later tests.
    bftExecutors.awaitStop();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void stopReturnsPromptlyWhenEnabledButNeverStarted() throws InterruptedException {
    // enable() alone reaches IDLE without start() ever having run: BftProcessor.run() never
    // executed, so its shutdownLatch would never be counted down. stop() must not block on
    // BftProcessor.awaitStop() in this case, or this test would hang until the @Timeout fires.
    final BftEventQueue eventQueue = new BftEventQueue(1000);
    final BftExecutors bftExecutors =
        BftExecutors.create(new NoOpMetricsSystem(), BftExecutors.ConsensusType.QBFT);
    final EventMultiplexer eventMultiplexer = new EventMultiplexer(eventHandler);
    final BftProcessor bftProcessor = new BftProcessor(eventQueue, eventMultiplexer);
    final BftMiningCoordinator coordinator =
        new BftMiningCoordinator(
            bftExecutors, eventHandler, bftProcessor, blockCreatorFactory, blockchain, eventQueue);

    coordinator.enable();
    coordinator.stop();

    assertThat(coordinator.isMining()).isFalse();
  }

  @Test
  @Timeout(value = 15, unit = TimeUnit.SECONDS)
  public void startWaitsForInFlightTeardownFromPriorEventThreadStop() throws InterruptedException {
    final BftEventQueue eventQueue = new BftEventQueue(1000);
    eventQueue.start();
    final BftExecutors bftExecutors =
        BftExecutors.create(new NoOpMetricsSystem(), BftExecutors.ConsensusType.QBFT);

    // Block completeStop() part-way through, inside eventHandler.stop(), so the test can
    // deterministically observe "teardown is in flight" before letting it finish.
    final CountDownLatch teardownStarted = new CountDownLatch(1);
    final CountDownLatch releaseTeardown = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              teardownStarted.countDown();
              releaseTeardown.await();
              return null;
            })
        .when(eventHandler)
        .stop();

    final AtomicReference<BftMiningCoordinator> coordinatorRef = new AtomicReference<>();
    // Simulates the TTD watcher: disable() then stop(), invoked on the BFT event thread.
    final EventMultiplexer eventMultiplexer =
        new EventMultiplexer(eventHandler) {
          @Override
          public void handleBftEvent(final BftEvent bftEvent) {
            coordinatorRef.get().disable();
            coordinatorRef.get().stop();
          }
        };

    final BftProcessor bftProcessor = new BftProcessor(eventQueue, eventMultiplexer);
    final BftMiningCoordinator coordinator =
        new BftMiningCoordinator(
            bftExecutors, eventHandler, bftProcessor, blockCreatorFactory, blockchain, eventQueue);
    coordinatorRef.set(coordinator);

    coordinator.enable();
    coordinator.start();
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));

    // Wait until stop()'s async teardown thread is blocked inside eventHandler.stop().
    assertThat(teardownStarted.await(10, TimeUnit.SECONDS)).isTrue();

    // A concurrent restart must block until that teardown finishes, not race ahead of it.
    final AtomicBoolean startReturned = new AtomicBoolean(false);
    final Thread restarter =
        new Thread(
            () -> {
              coordinator.start();
              startReturned.set(true);
            });
    restarter.start();

    // Give start() a chance to (wrongly) return early if it doesn't wait for the teardown.
    Thread.sleep(500);
    assertThat(startReturned.get()).isFalse();

    releaseTeardown.countDown();
    restarter.join(TimeUnit.SECONDS.toMillis(10));
    assertThat(startReturned.get()).isTrue();

    // The restarted cycle is genuinely running and was never told to stop; tear it down
    // explicitly (this stop() runs on the test thread, not the event thread, so it completes
    // its teardown inline) before awaiting so no threads leak into later tests.
    coordinator.stop();
    bftExecutors.awaitStop();
  }

  /**
   * Reproduces the exact scenario raised in PR review (besu-eth/besu#10733, discussion
   * r3543146286):
   *
   * <ol>
   *   <li>state = RUNNING
   *   <li>Thread A calls stop(): CAS(RUNNING to STOPPED) succeeds, and is now down in teardown
   *       (awaitStop() etc. - which takes real time)
   *   <li>Thread B calls start(): CAS(STOPPED to RUNNING) succeeds, bftProcessor.start()...
   *   <li>Thread C calls stop(): CAS(RUNNING to STOPPED) succeeds, enters teardown while A is still
   *       in its teardown
   * </ol>
   *
   * Both A and C are the BFT event thread calling stop() reentrantly (the merge transition
   * watcher's actual calling pattern), which is when stop() defers its teardown and this overlap
   * would be possible without start()'s pendingTeardown.join(). This test proves A's and C's
   * teardowns never run concurrently.
   */
  @Test
  @Timeout(value = 20, unit = TimeUnit.SECONDS)
  public void concurrentStopStartStopNeverOverlapsTeardowns() throws InterruptedException {
    final BftEventQueue eventQueue = new BftEventQueue(1000);
    eventQueue.start();
    final BftExecutors bftExecutors =
        BftExecutors.create(new NoOpMetricsSystem(), BftExecutors.ConsensusType.QBFT);

    final AtomicInteger activeTeardowns = new AtomicInteger(0);
    final AtomicInteger maxActiveTeardowns = new AtomicInteger(0);
    final CountDownLatch firstTeardownEntered = new CountDownLatch(1);

    // Every teardown (eventHandler.stop() call) takes a fixed amount of real time. This widens
    // the window in which a second, concurrently-dispatched teardown could overlap with it if
    // start() failed to wait for a prior one to finish - an overlap between two independent
    // async tasks is timing-dependent, so a wide, fixed window makes it reliably observable
    // instead of depending on incidental scheduling luck.
    doAnswer(
            invocation -> {
              final int active = activeTeardowns.incrementAndGet();
              maxActiveTeardowns.updateAndGet(max -> Math.max(max, active));
              firstTeardownEntered.countDown();
              try {
                Thread.sleep(300);
              } finally {
                activeTeardowns.decrementAndGet();
              }
              return null;
            })
        .when(eventHandler)
        .stop();

    final AtomicReference<BftMiningCoordinator> coordinatorRef = new AtomicReference<>();
    // Every dispatched event stops the coordinator reentrantly from the BFT event thread,
    // mirroring the merge transition watcher's calling pattern for both Thread A and Thread C.
    final EventMultiplexer eventMultiplexer =
        new EventMultiplexer(eventHandler) {
          @Override
          public void handleBftEvent(final BftEvent bftEvent) {
            coordinatorRef.get().stop();
          }
        };

    final BftProcessor bftProcessor = new BftProcessor(eventQueue, eventMultiplexer);
    final BftMiningCoordinator coordinator =
        new BftMiningCoordinator(
            bftExecutors, eventHandler, bftProcessor, blockCreatorFactory, blockchain, eventQueue);
    coordinatorRef.set(coordinator);

    coordinator.enable();
    coordinator.start();
    // Thread A: the event thread dispatches this event, calling stop() reentrantly.
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));

    assertThat(firstTeardownEntered.await(10, TimeUnit.SECONDS)).isTrue();

    // Thread B: a concurrent restart while A's teardown is still sleeping.
    final Thread threadB = new Thread(coordinator::start);
    threadB.start();

    // A correct start() blocks for close to the full 300 ms teardown; give it a much shorter
    // window to (wrongly) race ahead if it doesn't wait for A's teardown at all.
    Thread.sleep(50);
    assertThat(threadB.isAlive()).isTrue();

    threadB.join(TimeUnit.SECONDS.toMillis(10));
    assertThat(threadB.isAlive()).isFalse();
    assertThat(coordinator.isMining()).isTrue();

    // Thread C: the newly-restarted event thread dispatches another event immediately after B's
    // restart succeeded, calling stop() reentrantly again - while, if unguarded, A's teardown
    // could still be sleeping.
    eventQueue.add(new BlockTimerExpiry(new ConsensusRoundIdentifier(1, 0)));
    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> !coordinator.isMining());
    bftExecutors.awaitStop();

    // The guarantee under test: A's and C's teardowns never executed concurrently.
    assertThat(maxActiveTeardowns.get()).isEqualTo(1);
  }
}
