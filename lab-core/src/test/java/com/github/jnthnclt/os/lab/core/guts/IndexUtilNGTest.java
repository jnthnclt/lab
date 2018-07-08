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
package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.base.BolBuffer;
import com.github.jnthnclt.os.lab.base.IndexUtil;
import com.github.jnthnclt.os.lab.base.UIO;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class IndexUtilNGTest {

    @Test
    public void testCompare_6args() {
        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> IndexUtil.compare(UIO.longBytes(o1), 0, 8, UIO.longBytes(o2), 0, 8));

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }

    }

    @Test
    public void testCompare_ByteBuffer_ByteBuffer() {
        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, (o1, o2) -> IndexUtil.compare(new BolBuffer(UIO.longBytes(o1)), new BolBuffer(UIO.longBytes(o2))));

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

}
