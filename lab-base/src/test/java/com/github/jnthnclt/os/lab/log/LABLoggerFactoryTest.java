package com.github.jnthnclt.os.lab.log;

import com.github.jnthnclt.os.lab.log.LABLoggerFactory.SysoutLABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory.SysoutLABLoggerLevel;
import org.testng.annotations.Test;

public class LABLoggerFactoryTest {
    @Test
    public void testGetLogger() throws Exception {

        LABLoggerFactory.LAB_LOGGER_PROVIDER.set(
            name -> new SysoutLABLogger(name, SysoutLABLoggerLevel.DEBUG));

        LABLogger l = LABLoggerFactory.getLogger();
        l.debug("Debug");
        l.debug("Debug", new RuntimeException());
        l.debug("Debug {}", "1");
        l.debug("Debug {} {}", "1", "2");
        l.debug("Debug {} {} {}", "1", "2", "3");
        l.debug("Debug {} {} {} {}", "1", "2", "3", "4");
        l.debug("Debug {} {}", new Object[] { "1", "2" }, new RuntimeException());

        l.warn("Warn");
        l.warn("Warn", new RuntimeException());
        l.warn("Warn {}", "1");
        l.warn("Warn {} {}", "1", "2");
        l.warn("Warn {} {} {}", "1", "2", "3");
        l.warn("Warn {} {} {} {}", "1", "2", "3", "4");
        l.warn("Warn {} {}", new Object[] { "1", "2" }, new RuntimeException());

        l.error("Error");
        l.error("Error", new RuntimeException());
        l.error("Error {}", "1");
        l.error("Error {} {}", "1", "2");
        l.error("Error {} {} {}", "1", "2", "3");
        l.error("Error {} {} {} {}", "1", "2", "3", "4");
        l.error("Error {} {}", new Object[] { "1", "2" }, new RuntimeException());

        l.info("Info");
        l.info("Info", new RuntimeException());
        l.info("Info {}", "1");
        l.info("Info {} {}", "1", "2");
        l.info("Info {} {} {}", "1", "2", "3");
        l.info("Info {} {} {} {}", "1", "2", "3", "4");
        l.info("Info {} {}", new Object[] { "1", "2" }, new RuntimeException());

        l.info("Info {}", new boolean[]{true,false,true});
        l.info("Info {}", new byte[]{1,2,3});
        l.info("Info {}", new char[]{1,2,3});
        l.info("Info {}", new short[]{1,2,3});
        l.info("Info {}", new int[]{1,2,3});
        l.info("Info {}", new long[]{1,2,3});
        l.info("Info {}", new float[]{1,2,3});
        l.info("Info {}", new double[]{1,2,3});

        l.inc("Foo");
        l.inc("Foo", 1);
        l.set("Foo", 1);
        l.incAtomic("Foo");
        l.decAtomic("Foo");


    }

}