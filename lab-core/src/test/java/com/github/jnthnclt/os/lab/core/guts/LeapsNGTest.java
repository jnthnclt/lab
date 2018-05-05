package com.github.jnthnclt.os.lab.core.guts;

import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.google.common.io.Files;
import java.io.File;
import java.nio.ByteBuffer;
import com.github.jnthnclt.os.lab.core.io.BolBuffer;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LeapsNGTest {

    @Test
    public void testSomeMethod() throws Exception {

        Leaps write = new Leaps(1,
            new BolBuffer(UIO.longBytes(1)),
            new long[]{1, 2},
            new BolBuffer[]{new BolBuffer(UIO.longBytes(1)), new BolBuffer(UIO.longBytes(2))}, (readable) -> ByteBuffer.wrap(UIO.longsBytes(new long[]{1, 2}))
            .asLongBuffer());

        File file = new File(Files.createTempDir(), "leaps.bin");

        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
        IAppendOnly appendOnly = appendOnlyFile.appender();

        write.write(appendOnly);
        appendOnlyFile.flush(true);
        appendOnlyFile.close();

        ReadOnlyFile indexFile = new ReadOnlyFile(file);
        PointerReadableByteBufferFile pointerReadable = indexFile.pointerReadable(-1);
        Leaps read = Leaps.read(pointerReadable, 0);

        System.out.println("write:" + write.toString(null));
        System.out.println("read:" + read.toString(pointerReadable));

        Assert.assertEquals(write.toString(null), read.toString(pointerReadable));

    }
}
