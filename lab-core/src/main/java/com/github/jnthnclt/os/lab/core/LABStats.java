package com.github.jnthnclt.os.lab.core;

import com.github.jnthnclt.os.lab.log.LABLogger;
import com.github.jnthnclt.os.lab.log.LABLoggerFactory;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 *
 * @author jonathan.colt
 */
public class LABStats {

    private static final LABLogger LOG = LABLoggerFactory.getLogger();

    public final LongAdder debt = new LongAdder();
    public final LongAdder open = new LongAdder();
    public final LongAdder closed = new LongAdder();

    public final LongAdder append = new LongAdder();
    public final LongAdder journaledAppend = new LongAdder();

    public final LongAdder gets = new LongAdder();
    public final LongAdder rangeScan = new LongAdder();
    public final LongAdder multiRangeScan = new LongAdder();
    public final LongAdder rowScan = new LongAdder();

    public final AtomicLong merging = new AtomicLong();
    public final AtomicLong spliting = new AtomicLong();

    public final LongAdder slabbed = new LongAdder();
    public final LongAdder allocationed = new LongAdder();
    public final LongAdder released = new LongAdder();
    public final LongAdder freed = new LongAdder();

    public final LongAdder gc = new LongAdder();
    public final LongAdder gcCommit = new LongAdder();
    public final LongAdder pressureCommit = new LongAdder();
    public final LongAdder commit = new LongAdder();
    public final LongAdder fsyncedCommit = new LongAdder();

    public final LongAdder bytesWrittenToWAL = new LongAdder();
    public final LongAdder bytesWrittenAsIndex = new LongAdder();
    public final LongAdder bytesWrittenAsSplit = new LongAdder();
    public final LongAdder bytesWrittenAsMerge = new LongAdder();

    public final AtomicLong commitable = new AtomicLong();


    public LABStats(AtomicLong globalHeapCostInBytes) {

        register("heap>pressure", new LongCounter(globalHeapCostInBytes));
        register("lab>commitable", new LongCounter(commitable));

        register("files>debt", new LongAdderCounter(debt));
        register("files>open", new LongAdderCounter(open));
        register("files>closed", new LongAdderCounter(closed));

        register("append>append", new LongAdderCounter(append));
        register("append>journaledAppend", new LongAdderCounter(journaledAppend));

        register("read>gets", new LongAdderCounter(gets));
        register("read>rangeScan", new LongAdderCounter(rangeScan));
        register("read>multiRangeScan", new LongAdderCounter(multiRangeScan));
        register("read>rowScan", new LongAdderCounter(rowScan));

        register("lsm>merging", new LongCounter(merging));
        register("lsm>spliting", new LongCounter(spliting));

        register("memory.slabbed", new LongAdderCounter(slabbed));
        register("memory.allocationed", new LongAdderCounter(allocationed));
        register("memory.released", new LongAdderCounter(released));
        register("memory.freed", new LongAdderCounter(freed));


        register("commits>gc", new LongAdderCounter(gc));
        register("commits>gcCommit", new LongAdderCounter(gcCommit));
        register("commits>pressureCommit", new LongAdderCounter(pressureCommit));
        register("commits>commit", new LongAdderCounter(commit));
        register("commits>fsyncedCommit", new LongAdderCounter(fsyncedCommit));

        register("bytes>writtenToWAL", new LongAdderCounter(bytesWrittenToWAL));
        register("bytes>writtenAsIndex", new LongAdderCounter(bytesWrittenAsIndex));
        register("bytes>writtenAsSplit", new LongAdderCounter(bytesWrittenAsSplit));
        register("bytes>writtenAsMerge", new LongAdderCounter(bytesWrittenAsMerge));

    }

    public class LongCounter implements LABCounterMXBean {
        private final AtomicLong atomicLong;

        public LongCounter(AtomicLong atomicLong) {
            this.atomicLong = atomicLong;
        }

        @Override
        public long getValue() {
            return atomicLong.get();
        }

        @Override
        public String getType() {
            return "count";
        }
    }

    public class LongAdderCounter implements LABCounterMXBean {
        private final LongAdder longAdder;

        public LongAdderCounter(LongAdder longAdder) {
            this.longAdder = longAdder;
        }

        @Override
        public long getValue() {
            return longAdder.sum();
        }

        @Override
        public String getType() {
            return "count";
        }
    }

    public interface LABCounterMXBean {

        long getValue();

        String getType();

    }


    private static void register(String name, Object mbean) {
        name = name.replace(':', '_');

        String[] parts = name.split(">");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("leaf");
            sb.append(i);
            sb.append("=");
            sb.append(parts[i]);
        }

        Class clazz = mbean.getClass();
        String objectName = "lab.metrics:type=" + clazz.getSimpleName() + "," + sb.toString();

        LOG.debug("registering bean: " + objectName);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try {
            ObjectName mbeanName = new ObjectName(objectName);

            // note: unregister any previous, as this may be a replacement
            if (mbs.isRegistered(mbeanName)) {
                mbs.unregisterMBean(mbeanName);
            }

            mbs.registerMBean(mbean, mbeanName);

            LOG.debug("registered bean: " + objectName);
        } catch (MalformedObjectNameException | NotCompliantMBeanException |
            InstanceAlreadyExistsException | InstanceNotFoundException | MBeanRegistrationException e) {
            LOG.warn("unable to register bean: " + objectName + "cause: " + e.getMessage(), e);
        }
    }

}
