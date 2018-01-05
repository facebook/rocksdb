// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Struct class representing the output of a compaction operation
 */
public class CompactionOutput {

  public final CompactionDecision decision;
  public final String newValue;
  public final String skipUntil;

  // We cache this to avoid an extra Java lookup from native side
  private final byte decisionValue;

  /**
   * Constructor for a Keep or Remove decision.
   */
  public CompactionOutput(CompactionDecision decision) {
    assert(decision.equals(CompactionDecision.kKeep) || decision.equals(CompactionDecision.kRemove));
    this.decision = decision;
    this.decisionValue = decision.getValue();
    this.newValue = null;
    this.skipUntil = null;
  }

  /**
   * Constructor for a ChangeValue or RemoveAndSkipUntil decision.
   *
   * @param decision      The decision
   * @param param         Will be treated as either the new value or skip until parameter depending on {@code decision}
   */
  public CompactionOutput(CompactionDecision decision, String param) {
    assert(decision.equals(CompactionDecision.kChangeValue) || decision.equals(CompactionDecision.kRemoveAndSkipUntil));
    this.decision = decision;
    this.decisionValue = decision.getValue();
    if(this.decision.equals(CompactionDecision.kChangeValue)) {
      this.newValue = param;
      this.skipUntil = null;
    } else {
      this.newValue = null;
      this.skipUntil = param;
    }
  }

}
