/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.consensus.common.bft.BftEventQueue;
import org.hyperledger.besu.consensus.common.bft.BftExecutors;
import org.hyperledger.besu.consensus.common.bft.BftProcessor;
import org.hyperledger.besu.consensus.common.bft.events.NewChainHead;
import org.hyperledger.besu.consensus.common.bft.statemachine.BftEventHandler;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Bft mining coordinator. */
public class BftMiningCoordinator implements MiningCoordinator, BlockAddedObserver {

  private enum State {
    /** Never enabled or started. */
    UNINITIALIZED,
    /** Idle state. */
    IDLE,
    /** Running state. */
    RUNNING,
    /** Stopped state. */
    STOPPED,
    /** Paused state. */
    PAUSED,
  }

  // Sentinel for "observeBlockAdded() has never been called", since 0 is a valid subscriber ID
  // that some other, unrelated observer could legitimately hold.
  private static final long NOT_REGISTERED = -1;

  private static final Logger LOG = LoggerFactory.getLogger(BftMiningCoordinator.class);

  private final BftEventHandler eventHandler;
  private final BftProcessor bftProcessor;
  private final BftBlockCreatorFactory<?> blockCreatorFactory;

  /** The Blockchain. */
  protected final Blockchain blockchain;

  private final BftEventQueue eventQueue;
  private final BftExecutors bftExecutors;

  private volatile long blockAddedObserverId = NOT_REGISTERED;
  private final AtomicReference<State> state = new AtomicReference<>(State.UNINITIALIZED);
  // The teardown dispatched by the most recent stop() call. Guarded by this object's monitor
  // together with start()/stop(), see start(). Starts pre-completed so start() never needs a
  // null check before the first stop().
  private CompletableFuture<Void> pendingTeardown = CompletableFuture.completedFuture(null);

  private SyncState syncState;

  /**
   * Instantiates a new Bft mining coordinator.
   *
   * @param bftExecutors the bft executors
   * @param eventHandler the event handler
   * @param bftProcessor the bft processor
   * @param blockCreatorFactory the block creator factory
   * @param blockchain the blockchain
   * @param eventQueue the event queue
   */
  public BftMiningCoordinator(
      final BftExecutors bftExecutors,
      final BftEventHandler eventHandler,
      final BftProcessor bftProcessor,
      final BftBlockCreatorFactory<?> blockCreatorFactory,
      final Blockchain blockchain,
      final BftEventQueue eventQueue) {
    this.bftExecutors = bftExecutors;
    this.eventHandler = eventHandler;
    this.bftProcessor = bftProcessor;
    this.blockCreatorFactory = blockCreatorFactory;
    this.eventQueue = eventQueue;

    this.blockchain = blockchain;
  }

  /**
   * Instantiates a new Bft mining coordinator.
   *
   * @param bftExecutors the bft executors
   * @param eventHandler the event handler
   * @param bftProcessor the bft processor
   * @param blockCreatorFactory the block creator factory
   * @param blockchain the blockchain
   * @param eventQueue the event queue
   * @param syncState the sync state
   */
  public BftMiningCoordinator(
      final BftExecutors bftExecutors,
      final BftEventHandler eventHandler,
      final BftProcessor bftProcessor,
      final BftBlockCreatorFactory<?> blockCreatorFactory,
      final Blockchain blockchain,
      final BftEventQueue eventQueue,
      final SyncState syncState) {
    this.bftExecutors = bftExecutors;
    this.eventHandler = eventHandler;
    this.bftProcessor = bftProcessor;
    this.blockCreatorFactory = blockCreatorFactory;
    this.eventQueue = eventQueue;

    this.blockchain = blockchain;
    this.syncState = syncState;
  }

  @Override
  public synchronized void start() {
    // Wait for any teardown left in flight by a prior stop() before re-initializing. This is a
    // no-op in the common case: stop() only leaves teardown in flight when it was unable to
    // block for it itself (see stop()); otherwise teardown is already complete by the time
    // stop() returns, and pendingTeardown is left at its already-completed default.
    //
    // NOTE: start() must never be called from the BFT event thread itself, or this would
    // deadlock waiting for a teardown that is, in turn, waiting for that same thread's event
    // loop to exit. No current caller does this (start() is only invoked from node-startup or
    // sync-status callback threads).
    pendingTeardown.join();
    if (state.compareAndSet(State.IDLE, State.RUNNING)
        || state.compareAndSet(State.STOPPED, State.RUNNING)) {
      bftProcessor.start();
      bftExecutors.start();
      blockAddedObserverId = blockchain.observeBlockAdded(this);
      eventHandler.start();
      bftExecutors.executeBftProcessor(bftProcessor);
    }
  }

  @Override
  public synchronized void stop() {
    // Stop from RUNNING, PAUSED, or IDLE: the merge transition watcher calls disable()
    // (RUNNING -> PAUSED) immediately before stop(), and disable()/enable() never actually
    // touch the processor/executors themselves (they only flip this state), so a coordinator
    // that was started and then disabled/re-enabled without an intervening stop() can still be
    // sitting in IDLE with its processor genuinely running. PAUSED and IDLE are also reachable
    // without ever having started (e.g. enable() alone, before start()); the teardown below is
    // still safe in that case: bftProcessor.awaitStop() returns immediately if its event loop
    // never ran, blockchain.removeObserver() is skipped when no observer was ever registered,
    // and eventHandler.stop()/bftExecutors.stop() are self-guarded no-ops when never started.
    if (state.compareAndSet(State.RUNNING, State.STOPPED)
        || state.compareAndSet(State.PAUSED, State.STOPPED)
        || state.compareAndSet(State.IDLE, State.STOPPED)) {
      if (blockAddedObserverId != NOT_REGISTERED) {
        blockchain.removeObserver(blockAddedObserverId);
      }
      // Signals shutdown synchronously: sets the processor's shutdown flag immediately, on
      // this thread, guaranteeing no further event is dispatched once this call returns - this
      // is essential when the merge transition watcher calls stop() from the BFT event thread
      // itself (via block-added observers fired while QBFT imports the terminal block).
      bftProcessor.stop();
      // stop() blocks the calling thread until the coordinator has genuinely stopped, unless
      // doing so is logically impossible: the merge transition watcher can invoke stop() from
      // the BFT event thread itself, and that thread can never wait for its own exit. That one
      // case, and only that case, defers the remaining teardown; start() waits for it (via
      // pendingTeardown.join()) before allowing a subsequent restart to proceed.
      if (bftProcessor.isEventThread()) {
        pendingTeardown = CompletableFuture.runAsync(this::completeStop);
      } else {
        completeStop();
      }
    }
  }

  private void completeStop() {
    // Make sure the processor has stopped before shutting down the executors
    try {
      bftProcessor.awaitStop();
    } catch (final InterruptedException e) {
      LOG.debug("Interrupted while waiting for BftProcessor to stop.", e);
      Thread.currentThread().interrupt();
    }
    eventHandler.stop();
    bftExecutors.stop();
  }

  @Override
  public void subscribe() {
    if (syncState == null) {
      return;
    }
    syncState.subscribeSyncStatus(
        _ -> {
          if (syncState.syncTarget().isPresent() || !syncState.isInitialSyncPhaseDone()) {
            // We're syncing so stop doing other stuff
            LOG.info("Stopping BFT mining coordinator while we are syncing");
            stop();
          } else {
            LOG.info("Starting BFT mining coordinator following sync");
            enable();
            start();
          }
        });

    syncState.subscribeCompletionReached(
        new BesuEvents.InitialSyncCompletionListener() {
          @Override
          public void onInitialSyncCompleted() {
            LOG.info("Starting BFT mining coordinator following initial sync");
            enable();
            start();
          }

          @Override
          public void onInitialSyncRestart() {
            // Nothing to do. The mining coordinator won't be started until
            // sync has completed.
          }
        });
  }

  @Override
  public void awaitStop() throws InterruptedException {
    bftExecutors.awaitStop();
  }

  @Override
  public boolean enable() {
    // Return true if we're already running or idle, or successfully switch to idle. UNINITIALIZED
    // (the initial state) is treated the same as PAUSED here: neither has ever been started.
    return state.get() == State.RUNNING
        || state.get() == State.IDLE
        || state.compareAndSet(State.PAUSED, State.IDLE)
        || state.compareAndSet(State.UNINITIALIZED, State.IDLE);
  }

  @Override
  public boolean disable() {
    // UNINITIALIZED (the initial state) is already at rest, same as PAUSED: report success
    // without transitioning, there being nothing to disable.
    return state.get() == State.PAUSED
        || state.get() == State.UNINITIALIZED
        || state.compareAndSet(State.IDLE, State.PAUSED)
        || state.compareAndSet(State.RUNNING, State.PAUSED);
  }

  @Override
  public boolean isMining() {
    return state.get() == State.RUNNING;
  }

  @Override
  public Wei getMinTransactionGasPrice() {
    return blockCreatorFactory.getMinTransactionGasPrice();
  }

  @Override
  public Wei getMinPriorityFeePerGas() {
    return blockCreatorFactory.getMinPriorityFeePerGas();
  }

  @Override
  public Optional<Block> createBlock(
      final BlockHeader parentHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers) {
    // One-off block creation has not been implemented
    return Optional.empty();
  }

  @Override
  public Optional<Block> createBlock(final BlockHeader parentHeader, final long timestamp) {
    // One-off block creation has not been implemented
    return Optional.empty();
  }

  @Override
  public void changeTargetGasLimit(final Long targetGasLimit) {
    blockCreatorFactory.changeTargetGasLimit(targetGasLimit);
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    if (event.isNewCanonicalHead()) {
      LOG.trace("New canonical head detected");
      eventQueue.add(new NewChainHead(event.getHeader()));
    }
  }

  @Override
  public void removeObserver() {
    blockchain.removeObserver(blockAddedObserverId);
  }
}
