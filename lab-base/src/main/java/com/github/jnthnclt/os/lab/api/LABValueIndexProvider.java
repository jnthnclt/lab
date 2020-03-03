package com.github.jnthnclt.os.lab.api;

import com.github.jnthnclt.os.lab.name.LABName;

public interface LABValueIndexProvider {

    ValueIndex getValueIndex(LABName name);
}
