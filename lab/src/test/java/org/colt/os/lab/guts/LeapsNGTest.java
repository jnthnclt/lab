package org.colt.os.lab.guts;

import com.google.common.io.Files;
import java.io.File;
import java.nio.ByteBuffer;
import org.colt.os.lab.api.FormatTransformer;
import org.colt.os.lab.io.BolBuffer;
import org.colt.os.lab.io.PointerReadableByteBufferFile;
import org.colt.os.lab.io.api.IAppendOnly;
import org.colt.os.lab.io.api.UIO;
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

        write.write(FormatTransformer.NO_OP, appendOnly);
        appendOnlyFile.flush(true);
        appendOnlyFile.close();

        ReadOnlyFile indexFile = new ReadOnlyFile(file);
        PointerReadableByteBufferFile pointerReadable = indexFile.pointerReadable(-1);
        Leaps read = Leaps.read(FormatTransformer.NO_OP, pointerReadable, 0);

        System.out.println("write:" + write.toString(null));
        System.out.println("read:" + read.toString(pointerReadable));

        Assert.assertEquals(write.toString(null), read.toString(pointerReadable));

    }
}
