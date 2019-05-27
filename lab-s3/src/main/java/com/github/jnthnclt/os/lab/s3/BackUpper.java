package com.github.jnthnclt.os.lab.s3;

import java.io.File;
import java.io.IOException;

public interface BackUpper {

    void backup(String key, File file);

    void delete(String key, File file);

    void restore(File indexRoot, String indexName) throws IOException;

}
