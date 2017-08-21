package com.github.jnthnclt.os.lab.core;

import com.google.common.io.Files;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABMetaNGTest {

    @Test
    public void testGet() throws Exception {
        File tmpDir = Files.createTempDir();

        LABMeta meta = new LABMeta(tmpDir);

        for (int i = 0; i < 10; i++) {
            meta.append(UIO.longBytes(i), UIO.longBytes(i), true);
        }

        for (int i = 0; i < 10; i++) {
            byte[] value = meta.get(UIO.longBytes(i), BolBuffer::copy);
            System.out.println("A:" + UIO.bytesLong(value) + " vs " + i);
            Assert.assertEquals(UIO.bytesLong(value), i);
        }

        for (int i = 0; i < 10; i++) {
            meta.append(UIO.longBytes(i), UIO.longBytes(i * 2), true);
        }

        for (int i = 0; i < 10; i++) {
            byte[] value = meta.get(UIO.longBytes(i), BolBuffer::copy);
            System.out.println("B:" + UIO.bytesLong(value) + " vs " + i * 2);
            Assert.assertEquals(UIO.bytesLong(value), i * 2);
        }

        meta.close();

        meta = new LABMeta(tmpDir);
        Set<Long> keys = new HashSet<>();
        meta.metaKeys((byte[] metaKey) -> {
            long k = UIO.bytesLong(metaKey);
            System.out.println("k:" + k);
            keys.add(k);
            return true;
        });

        Assert.assertTrue(keys.size() == 10);

        for (int i = 0; i < 10; i++) {
            meta.append(UIO.longBytes(i), UIO.longBytes(i * 3), true);
        }

        for (int i = 0; i < 10; i++) {
            byte[] value = meta.get(UIO.longBytes(i), BolBuffer::copy);
            Assert.assertEquals(UIO.bytesLong(value), i * 3);
        }

        meta.metaKeys((byte[] metaKey) -> {
            keys.add(UIO.bytesLong(metaKey));
            return true;
        });

        Assert.assertTrue(keys.size() == 10);

    }

}
