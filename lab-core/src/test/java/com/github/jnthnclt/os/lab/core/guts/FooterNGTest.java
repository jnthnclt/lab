package com.github.jnthnclt.os.lab.core.guts;

import com.google.common.io.Files;
import java.io.File;
import com.github.jnthnclt.os.lab.core.io.PointerReadableByteBufferFile;
import com.github.jnthnclt.os.lab.core.io.api.IAppendOnly;
import com.github.jnthnclt.os.lab.core.io.api.UIO;
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
