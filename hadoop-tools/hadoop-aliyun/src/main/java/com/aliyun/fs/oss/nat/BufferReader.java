package com.aliyun.fs.oss.nat;

import com.aliyun.fs.oss.common.NativeFileSystemStore;
import com.aliyun.fs.oss.utils.TaskEngine;
import com.aliyun.fs.oss.utils.task.Task;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * thread1 thread2        thread2^m
 *  |        |                |
 * /        /                /
 * [0][1][2][3][4][5][6][7]...|...[2^n-8][2^n-7][2^n-6][2^n-5][2^n-4][2^n-3][2^n-2][2^n-1]
 */
public class BufferReader {
    public static final Log LOG = LogFactory.getLog(BufferReader.class);

    private NativeFileSystemStore store;
    private int concurrentStreams;
    private Configuration conf;
    private TaskEngine taskEngine;
    private int bufferSize;
    private String key;
    private byte[] buffer;
    private Task[] readers;
    private int[] splitContentSize;
    private AtomicInteger halfHavePrepared = new AtomicInteger(1);
    private AtomicInteger halfReading = new AtomicInteger(0);
    private AtomicInteger ready0 = new AtomicInteger(0);
    private AtomicInteger ready1 = new AtomicInteger(0);
    private boolean closed = false;
    private int cacheIdx = 0;
    private int splitSize = 0;
    private long fileContentLength;
    private long pos = 0;
    private boolean squeezed0 = false;
    private boolean squeezed1 = false;
    private int realContentSize;
    private double lastProgress = 0.0d;

    public BufferReader(NativeFileSystemStore store, String key, Configuration conf) throws IOException {
        this.store = store;
        this.key = key;
        this.conf = conf;
        this.bufferSize = conf.getInt("fs.oss.readBuffer.size", 64 * 1024 * 1024);
        if (this.bufferSize % 2 != 0) {
            int power = (int) Math.ceil(Math.log(bufferSize) / Math.log(2));
            this.bufferSize = (int) Math.pow(2, power);
        }
        // do not suggest to use too large buffer in case of GC issue or OOM.
        if (this.bufferSize >= 256 * 1024 * 1024) {
            LOG.warn("'fs.oss.readBuffer.size' is " + bufferSize + ", it's to large and system will suppress it down " +
                    "to '268435456' automatically.");
            this.bufferSize = 256 * 1024 * 1024;
        }
        this.buffer = new byte[bufferSize];
        this.concurrentStreams = conf.getInt("fs.oss.reader.concurrent.number", 4);
        if ((Math.log(concurrentStreams) / Math.log(2)) != 0) {
            int power = (int) Math.ceil(Math.log(concurrentStreams) / Math.log(2));
            this.concurrentStreams = (int) Math.pow(2, power);
        }
        this.readers = new ConcurrentReader[concurrentStreams];
        this.splitContentSize = new int[concurrentStreams*2];
        this.splitSize = bufferSize / concurrentStreams / 2;
        this.fileContentLength = store.retrieveMetadata(key).getLength();

        initialize();
    }

    private void initialize() {
        for(int i=0; i<concurrentStreams; i++) {
            try {
                readers[i] = new ConcurrentReader(i);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        this.taskEngine = new TaskEngine(Arrays.asList(this.readers), concurrentStreams, concurrentStreams);
        this.taskEngine.executeTask();
    }

    public void close() {
        taskEngine.shutdown();
    }

    public synchronized int read() throws IOException {
        while (true) {
            if (halfReading.get() == 0) {
                int i = 0;
                while (!(ready0.get() == concurrentStreams)) {
                    i++;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                    if (i % 100 == 0) {
                        LOG.warn("waiting for fetching oss data, has completed " + ready0.get());
                    }
                }
                if (!squeezed0) {
                    realContentSize = squeeze();
                    squeezed0 = true;
                    squeezed1 = false;
                    progressPrint();
                }

                // read data from buffer half-0
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    int ret = buffer[cacheIdx];
                    cacheIdx++;
                    pos++;
                    return ret;
                } else {
                    ready0.set(0);
                    halfReading.set(1);
                    halfHavePrepared.addAndGet(1);
                    cacheIdx = 0;
                }
            } else {
                int i = 0;
                while (!(ready1.get() == concurrentStreams)) {
                    i++;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                    if (i % 100 == 0) {
                        LOG.warn("waiting for fetching oss data, has completed " + ready1.get());
                    }
                }
                if (!squeezed1) {
                    realContentSize = squeeze();
                    squeezed0 = false;
                    squeezed1 = true;
                    progressPrint();
                }

                // read data from buffer half-1
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    int ret = buffer[bufferSize / 2 + cacheIdx];
                    cacheIdx++;
                    return ret;
                } else {
                    ready1.set(0);
                    halfReading.set(0);
                    halfHavePrepared.addAndGet(1);
                    cacheIdx = 0;
                }
            }
        }
    }

    public synchronized int read(byte[] b, int off, int len) {
        while(true) {
            if (halfReading.get() == 0) {
                int j = 0;
                while (!(ready0.get() == concurrentStreams)) {
                    j++;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                    if (j % 100 == 0) {
                        LOG.warn("waiting for fetching oss data, has completed " + ready0.get());
                    }
                }
                if (!squeezed0) {
                    realContentSize = squeeze();
                    squeezed0 = true;
                    squeezed1 = false;
                    progressPrint();
                }

                // read data from buffer half-0
                int size = 0;
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    for (int i = 0; i < len && cacheIdx < realContentSize; i++) {
                        b[off + i] = buffer[cacheIdx];
                        cacheIdx++;
                        pos++;
                        size++;
                    }
                    return size;
                } else {
                    ready0.set(0);
                    halfReading.set(1);
                    halfHavePrepared.addAndGet(1);
                    cacheIdx = 0;
                }
            } else {
                int j = 0;
                while (!(ready1.get() == concurrentStreams)) {
                    j++;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                    if (j % 100 == 0) {
                        LOG.warn("waiting for fetching oss data, has completed " + ready1.get());
                    }
                }
                if (!squeezed1) {
                    realContentSize = squeeze();
                    squeezed0 = false;
                    squeezed1 = true;
                    progressPrint();
                }

                // read data from buffer half-1
                int size = 0;
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    for (int i = 0; i < len && cacheIdx < realContentSize; i++) {
                        b[off + i] = buffer[bufferSize / 2 + cacheIdx];
                        cacheIdx++;
                        pos++;
                        size++;
                    }
                    return size;
                } else {
                    ready1.set(0);
                    halfReading.set(0);
                    halfHavePrepared.addAndGet(1);
                    cacheIdx = 0;
                }
            }
        }
    }

    private int squeeze() {
        int totalSize = 0;
        int begin;
        if (halfReading.get() == 0) {
            for(int i=0; i<concurrentStreams; i++) {
                totalSize += splitContentSize[i];
            }
            begin = 0;

            int cacheIdx;
            if (totalSize != bufferSize/2) {
                cacheIdx = splitContentSize[0];
                for(int i=1; i <concurrentStreams; i++) {
                    for (int j=0; j<splitContentSize[i]; j++) {
                        buffer[begin+cacheIdx] = buffer[begin+splitSize*i+j];
                        cacheIdx++;
                    }
                }
            }
        } else {
            for(int i=0; i<concurrentStreams; i++) {
                totalSize += splitContentSize[concurrentStreams+i];
            }
            begin = bufferSize/2;

            int cacheIdx;
            if (totalSize != bufferSize/2) {
                cacheIdx = splitContentSize[concurrentStreams];
                for(int i=1; i <concurrentStreams; i++) {
                    for (int j=0; j<splitContentSize[concurrentStreams+i]; j++) {
                        buffer[begin+cacheIdx] = buffer[begin+splitSize*i+j];
                        cacheIdx++;
                    }
                }
            }
        }

        return totalSize;
    }

    private class ConcurrentReader extends Task {
        private final Log LOG = LogFactory.getLog(ConcurrentReader.class);
        private Boolean preRead = true;
        private int readerId = -1;
        private boolean half0Completed = false;
        private boolean half1Completed = false;
        private int half0StartPos = -1;
        private int half1StartPos = -1;
        private int length = -1;
        private boolean _continue = true;

        public ConcurrentReader(int readerId) throws FileNotFoundException {
            assert(bufferSize%2 == 0);
            assert(concurrentStreams%2 == 0);
            this.readerId = readerId;
            this.length = bufferSize / (2 * concurrentStreams);
            assert(concurrentStreams*length*2 == bufferSize);

            this.half0StartPos = readerId * length;
            this.half1StartPos = bufferSize / 2 + readerId * length;
        }

        @Override
        public void execute(TaskEngine engineRef) throws IOException {
            while (!closed && _continue) {
                if (preRead) {
                    // fetch oss data for half-0 and half-1 at the first time, as there is no data in buffer.
                    _continue = fetchData(half0StartPos);
                    half0Completed = true;
                    half1Completed = false;
                    ready0.addAndGet(1);
                    preRead = false;
                } else if (halfReading.get() == 0 && !half1Completed) {
                    // fetch oss data for half-1
                    _continue = fetchData(half1StartPos);
                    half1Completed = true;
                    half0Completed = false;
                    ready1.addAndGet(1);
                } else if (halfReading.get() == 1 && !half0Completed) {
                    // fetch oss data for half-0
                    _continue = fetchData(half0StartPos);
                    half0Completed = true;
                    half1Completed = false;
                    ready0.addAndGet(1);
                } else {
                    // waiting for `halfReading` block data to be consumed
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }

        private boolean fetchData(int startPos) throws IOException {
            boolean _continue = true;
            if (startPos == half0StartPos) {
                splitContentSize[readerId] = 0;
            } else {
                splitContentSize[concurrentStreams + readerId] = 0;
            }
            long newPos;
            int fetchLength;
            if (preRead && bufferSize / 2 >= fileContentLength) {
                _continue = false;
                fetchLength = (int) fileContentLength / concurrentStreams;
                newPos = fetchLength * readerId;
                if (readerId == (concurrentStreams-1)) {
                    fetchLength = (int) fileContentLength - fetchLength * (concurrentStreams - 1);
                }
            } else if (preRead) {
                fetchLength = bufferSize / (2*concurrentStreams);
                newPos = fetchLength * readerId;
            } else if ((long)(halfHavePrepared.get()+1) * bufferSize / 2 >= fileContentLength) {
                _continue = false;
                fetchLength = (int) (fileContentLength - (long) halfHavePrepared.get() * bufferSize / 2) / concurrentStreams;
                newPos = (long) halfHavePrepared.get() * bufferSize / 2 + readerId * fetchLength;
                if (readerId == (concurrentStreams-1)) {
                    fetchLength = (int) (fileContentLength - (long) halfHavePrepared.get() * bufferSize / 2 - (fetchLength * (concurrentStreams - 1)));
                }
            } else {
                fetchLength = bufferSize / (2*concurrentStreams);
                newPos = (long) halfHavePrepared.get() * bufferSize / 2 + readerId * fetchLength;
            }
            InputStream in;
            try {
                in = store.retrieve(key, newPos, fetchLength);
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
                throw new IOException("[ConcurrentReader-"+readerId+"] Cannot open oss input stream");
            }

            int off = startPos;
            int tries = 10;
            int result;
            boolean retry = true;
            int hasRead = 0;
            do {
                try {
                    result = in.read(buffer, off, fetchLength-hasRead);
                    if (result > 0) {
                        off += result;
                        hasRead += result;
                    } else if (result == -1) {
                        break;
                    }
                    retry = hasRead < fetchLength;
                } catch (EOFException e0) {
                    LOG.warn(e0.getMessage(), e0);
                    throw e0;
                } catch (Exception e1) {
                    tries--;
                    if (tries == 0) {
                        throw new IOException(e1);
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e2) {
                        LOG.warn(e2.getMessage());
                    }
                    if (in != null) {
                        try {
                            in.close();
                        } catch (Exception e) {
                            // do nothing
                        } finally {
                            in = null;
                        }
                    }
                    try {
                        in = store.retrieve(key, newPos, fetchLength);
                    } catch (Exception e) {
                        LOG.warn(e.getMessage(), e);
                        throw new IOException("[ConcurrentReader-"+readerId+"] Cannot open oss input stream", e);
                    }
                    off = startPos;
                    hasRead = 0;
                }
            } while (tries>0 && retry);
            in.close();
            if (startPos == half0StartPos) {
                splitContentSize[readerId] = hasRead;
            } else {
                splitContentSize[concurrentStreams + readerId] = hasRead;
            }

            return _continue;
        }
    }

    private void progressPrint() {
        long hasRead = (long) halfHavePrepared.get() * bufferSize / 2;
        double currentProgress = hasRead > fileContentLength ? 1.0d : (double) hasRead / fileContentLength;
        if (currentProgress - lastProgress >= 0.1 || currentProgress == 1.0d) {
            BigDecimal b = new BigDecimal(currentProgress);
            LOG.info("Current progress of reading '" + key + "' is: " + b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
            lastProgress = currentProgress;
        }

        if (currentProgress == 1.0d) {
            taskEngine.shutdown();
        }
    }
}
