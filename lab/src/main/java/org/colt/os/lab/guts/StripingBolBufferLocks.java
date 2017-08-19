/*
 * Copyright 2016 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.colt.os.lab.guts;

import org.colt.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public class StripingBolBufferLocks {

    private final StripingBolBufferLock[] locks;

    public StripingBolBufferLocks(int numLocks) {
        locks = new StripingBolBufferLock[numLocks];
        for (int i = 0; i < numLocks; i++) {
            locks[i] = new StripingBolBufferLock();
        }
    }

    public Object lock(BolBuffer buffer, int seed) {
        return locks[Math.abs(((int)buffer.longHashCode() ^ seed) % locks.length)];
    }

    static private class StripingBolBufferLock {
    }

}
