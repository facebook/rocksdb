/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Range {

    final private byte[] start;
    final private byte[] limit;

    public byte[] limit() {
        return limit;
    }

    public byte[] start() {
        return start;
    }

    public Range(byte[] start, byte[] limit) {
        Options.checkArgNotNull(start, "start");
        Options.checkArgNotNull(limit, "limit");
        this.limit = limit;
        this.start = start;
    }

}
