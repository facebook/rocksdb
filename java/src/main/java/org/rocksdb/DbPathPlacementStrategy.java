// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Enum DbPathPlacementStrategy
 *
 * RocksDB supports using multiple paths for storing its data files.
 * Currently there are two ways of using these paths:
 *
 * <ol>
 *   <li><strong>GRADUAL_MOVE_OLD_DATA_TOWARDS_END</strong> - Fill DbPaths
 *   according to the target size set with the db path. Newer data is placed
 *   into paths specified earlier in the vector while older data gradually
 *   moves to paths specified later in the vector.
 *   <br>
 *   For example, you have a flash device with 10GB allocated for the DB,
 *   as well as a hard drive of 2TB, you should config it to be:
 *     [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
 *   <br>
 *   The system will try to guarantee data under each path is close to but
 *   not larger than the target size. But current and future file sizes used
 *   by determining where to place a file are based on best-effort estimation,
 *   which means there is a chance that the actual size under the directory
 *   is slightly more than target size under some workloads. User should give
 *   some buffer room for those cases.
 *   <br>
 *   If none of the paths has sufficient room to place a file, the file will
 *   be placed to the last path anyway, despite to the target size.
 *   <br>
 *   Placing newer data to earlier paths is also best-efforts. User should
 *   expect user files to be placed in higher levels in some extreme cases.
 *   </li>
 *
 *   <li><strong>RANDOMLY_CHOOSE_PATH</strong> - Randomly distribute files
 *   into the list of db paths.
 *   <br>
 *   For example, you have a few data drives on your host that are mounted
 *   as [/sdb1, /sdc1, /sdd1, /sde1]. Say that the database will create 6
 *   table files -- 0[0-5].sst, then they will end up on in these places:
 *   <br>
 *   /sdb1/02.sst <br>
 *   /sdb1/04.sst <br>
 *   /sdc1/05.sst <br>
 *   /sdc1/03.sst <br>
 *   /sdd1/00.sst <br>
 *   /sde1/01.sst <br>
 *   <br>
 *   This is useful if you want the database to evenly use a set of disks
 *   mounted on your host.
 *   <br>
 *   Note that the target_size attr in DbPath will not be useful if this
 *   strategy is chosen.
 *   </li>
 * </ol>
 */
public enum DbPathPlacementStrategy {
  GRADUAL_MOVE_OLD_DATA_TOWARDS_END((byte) 0),
  RANDOMLY_CHOOSE_PATH((byte) 1);

  private final byte value_;

  private DbPathPlacementStrategy(byte value) {
    value_ = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * Parse the byte representation of a strategy and return the enum value.
   *
   * @return the strategy
   */
  static DbPathPlacementStrategy fromValue(byte value) {
    switch(value) {
    case (byte) 0:
      return GRADUAL_MOVE_OLD_DATA_TOWARDS_END;
    case (byte) 1:
      return RANDOMLY_CHOOSE_PATH;
    default:
      throw new IllegalArgumentException("cannot parse value " + value);
    }
  }
}
