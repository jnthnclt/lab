package com.github.jnthnclt.os.lab.core.io;

import com.github.jnthnclt.os.lab.core.guts.AppendOnlyFile;
import com.github.jnthnclt.os.lab.core.guts.ReadOnlyFile;
import com.google.common.io.Files;
import java.io.File;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class PointerReadableByteBufferFileNGTest {

    @Test
    public void test() throws Exception {
        File file = new File(Files.createTempDir(), "pointerReadableByteBuffer.bin");

        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
        IAppendOnly appendOnly = appendOnlyFile.appender();

        for (int i = 0; i < 1_000; i++) {
            appendOnly.append(UIO.longBytes(i), 0, 8);
        }
        appendOnly.flush(true);
        appendOnlyFile.close();

        ReadOnlyFile readOnlyFile = new ReadOnlyFile(file);
        PointerReadableByteBufferFile pointerReadable = readOnlyFile.pointerReadable(33);

        int offset = 0;
        for (int i = 0; i < 1_000; i++) {
            long l = pointerReadable.readLong(offset);
            offset += 8;
            Assert.assertEquals(l, i);
        }

        offset = 0;
        for (int i = 0; i < 1_000; i++) {
            long a = pointerReadable.readInt(offset);
            offset += 4;
            Assert.assertEquals(a, 0);
            long b = pointerReadable.readInt(offset);
            offset += 4;
            Assert.assertEquals(b, i);
        }

        offset = 0;
        byte[] readBytes = new byte[8 * 4];
        for (int i = 0; i < 1_000 - 4; i++) {
            pointerReadable.read(offset, readBytes, 0, readBytes.length);
            offset += 8;

            Assert.assertEquals(UIO.bytesLong(readBytes, 0), i);
            Assert.assertEquals(UIO.bytesLong(readBytes, 8), i + 1);
            Assert.assertEquals(UIO.bytesLong(readBytes, 16), i + 2);
            Assert.assertEquals(UIO.bytesLong(readBytes, 24), i + 3);
        }

        offset = 0;
        for (int i = 0; i < 1_000; i++) {
            byte[] expected = new byte[8];
            UIO.longBytes(i, expected, 0);

            Assert.assertEquals(expected[0], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[1], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[2], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[3], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[4], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[5], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[6], (byte)pointerReadable.read(offset));
            offset++;
            Assert.assertEquals(expected[7], (byte)pointerReadable.read(offset));
            offset++;

        }

        BolBuffer bolBuffer = new BolBuffer();
        offset = 0;
        for (int i = 0; i < 1_000 - 4; i++) {
            pointerReadable.sliceIntoBuffer(offset, offset, bolBuffer);
            offset += 8;

            Assert.assertEquals(bolBuffer.getLong(0), i);
            Assert.assertEquals(bolBuffer.getLong(8), i + 1);
            Assert.assertEquals(bolBuffer.getLong(16), i + 2);
            Assert.assertEquals(bolBuffer.getLong(24), i + 3);
        }

    }

}
