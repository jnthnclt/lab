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
    public final LongAdder appendSlowed = new LongAdder();
    public final LongAdder journaledAppend = new LongAdder();

    public final LongAdder gets = new LongAdder();
    public final LongAdder rangeScan = new LongAdder();
    public final LongAdder pointRangeScan = new LongAdder();
    public final LongAdder multiRangeScan = new LongAdder();
    public final LongAdder rowScan = new LongAdder();

    public final AtomicLong merging = new AtomicLong();
    public final AtomicLong spliting = new AtomicLong();

    public final AtomicLong merged = new AtomicLong();
    public final AtomicLong split = new AtomicLong();


    public final LongAdder slabbed = new LongAdder();
    public final LongAdder allocated = new LongAdder();
    public final LongAdder released = new LongAdder();
    public final LongAdder freed = new LongAdder();

    public final LongAdder gc = new LongAdder();
    public final LongAdder gcCommit = new LongAdder();
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

        register("files>debt", new LongCounter(debt));
        register("files>open", new LongCounter(open));
        register("files>closed", new LongCounter(closed));

        register("append>append", new LongCounter(append));
        register("append>journaledAppend", new LongCounter(journaledAppend));
        register("append>appendSlowed", new LongCounter(appendSlowed));

        register("read>gets", new LongCounter(gets));
        register("read>rangeScan", new LongCounter(rangeScan));
        register("read>pointRangeScan", new LongCounter(pointRangeScan));
        register("read>multiRangeScan", new LongCounter(multiRangeScan));
        register("read>rowScan", new LongCounter(rowScan));

        register("lsm>merging", new LongCounter(merging));
        register("lsm>spliting", new LongCounter(spliting));

        register("lsm>merged", new LongCounter(merged));
        register("lsm>split", new LongCounter(split));


        register("memory>slabbed", new LongCounter(slabbed));
        register("memory>allocationed", new LongCounter(allocated));
        register("memory>released", new LongCounter(released));
        register("memory>freed", new LongCounter(freed));


        register("commits>gc", new LongCounter(gc));
        register("commits>gcCommit", new LongCounter(gcCommit));
        register("commits>commit", new LongCounter(commit));
        register("commits>fsyncedCommit", new LongCounter(fsyncedCommit));

        register("bytes>writtenToWAL", new LongCounter(bytesWrittenToWAL));
        register("bytes>writtenAsIndex", new LongCounter(bytesWrittenAsIndex));
        register("bytes>writtenAsSplit", new LongCounter(bytesWrittenAsSplit));
        register("bytes>writtenAsMerge", new LongCounter(bytesWrittenAsMerge));

    }

    public class LongCounter implements LABCounterMXBean {
        private final Number number;

        public LongCounter(Number number) {
            this.number = number;
        }

        @Override
        public long getValue() {
            return number.longValue();
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
