/*
 * IFilerBytes.java.java
 *
 * Created on 03-12-2010 11:13:39 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jnthnclt.os.lab.io.filer;

import java.io.IOException;

/**
 * @author Administrator
 */
public interface IFilerBytes extends IReadable, IWriteableBytes {

    /**
     *
     */
    final public static String cRead = "r";
    /**
     *
     */
    final public static String cWrite = "rw";
    /**
     *
     */
    final public static String cReadWrite = "rw";

    /**
     * @param position
     * @return
     * @throws java.io.IOException
     */
    long skip(long position) throws IOException;

    /**
     * @param len
     * @throws java.io.IOException
     */
    void setLength(long len) throws IOException;

    /**
     * @throws java.io.IOException
     */
    void eof() throws IOException;

}
