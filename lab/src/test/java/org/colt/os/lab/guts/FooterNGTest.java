package org.colt.os.lab.guts;

import com.google.common.io.Files;
import java.io.File;
import org.colt.os.lab.io.PointerReadableByteBufferFile;
import org.colt.os.lab.io.api.IAppendOnly;
import org.colt.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class FooterNGTest {

    @Test
    public void testToString() throws Exception {
        Footer write = new Footer(1, 2, 3, 4, UIO.longsBytes(new long[]{1, 2, 3, 4}), UIO.longsBytes(new long[]{4, 5}), 5, 6, 7, 8);

        File file = new File(Files.createTempDir(), "footer.bin");

        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(file);
        IAppendOnly appendOnly = appendOnlyFile.appender();

       
        write.write(appendOnly);
        appendOnly.flush(true);
        appendOnly.close();

        ReadOnlyFile indexFile = new ReadOnlyFile(file);
        PointerReadableByteBufferFile pointerReadable = indexFile.pointerReadable(-1);
        Footer read = Footer.read(pointerReadable, 0);

        System.out.println("write:" + write.toString());
        System.out.println("read:" + read.toString());

        Assert.assertEquals(write.toString(), read.toString());

    }
}
