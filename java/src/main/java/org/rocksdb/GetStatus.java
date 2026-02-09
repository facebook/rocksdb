//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The result for a fetch
 * and the total size of the object fetched.
 * If the target of the fetch is not big enough, this may be bigger than the contents of the target.
 */
public class GetStatus {
  public final Status status;
  public final int requiredSize;

  /**
   * Constructor used for success status, when the value is contained in the buffer
   *
   * @param status the status of the request to fetch into the buffer
   * @param requiredSize the size of the data, which may be bigger than the buffer
   */
  GetStatus(final Status status, final int requiredSize) {
    this.status = status;
    this.requiredSize = requiredSize;
  }

  static GetStatus fromStatusCode(final Status.Code code, final int requiredSize) {
    return new GetStatus(new Status(code, Status.SubCode.getSubCode((byte) 0), null), requiredSize);
  }
}
