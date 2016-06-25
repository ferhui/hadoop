package com.aliyun.fs.oss.nat;

import com.aliyun.fs.oss.common.NativeFileSystemStore;
import com.aliyun.fs.oss.utils.TaskEngine;
import com.aliyun.fs.oss.utils.task.Task;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * thread1 thread2
 *  |        |
 * /        /
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
    private int startPos0;
    private int endPos0;
    private int startPos1;
    private int endPos1;
    private AtomicInteger halfHaveConsumed = new AtomicInteger(0);
    private AtomicInteger halfReading = new AtomicInteger(0);
    private AtomicInteger ready0 = new AtomicInteger(0);
    private AtomicInteger ready1 = new AtomicInteger(0);
    private boolean closed = false;
    private int cacheIdx = 0;
    private int splitSize = 0;

    public BufferReader(NativeFileSystemStore store, String key, Configuration conf) {
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
        this.concurrentStreams = conf.getInt("fs.oss.read.concurrent.number", 4);
        if (this.concurrentStreams % 2 != 0) {
            int power = (int) Math.ceil(Math.log(concurrentStreams) / Math.log(2));
            this.concurrentStreams = (int) Math.pow(2, power);
        }
        this.readers = new ConcurrentReader[concurrentStreams];
        this.splitContentSize = new int[concurrentStreams*2];
        int readableBufferSize = bufferSize / 2;
        this.startPos0 = 0;
        this.endPos0 = readableBufferSize - 1;
        this.startPos1 = readableBufferSize;
        this.endPos1 = bufferSize - 1;
        this.splitSize = bufferSize / concurrentStreams / 2;

        initialize();
    }

    private void initialize() {
        for(int i=0; i<5; i++) {
            readers[i] = new ConcurrentReader(i);
        }
        this.taskEngine = new TaskEngine(Arrays.asList(this.readers), 5, 5);
        this.taskEngine.executeTask();
    }

    public void close() {
        taskEngine.shutdown();
    }

    public synchronized int read() throws IOException {
        if (halfReading.get() == 0) {
            while (!(ready0.get() == concurrentStreams)) {
                LOG.warn("waiting for fetching oss data.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn("Something wrong, keep waiting.");
                }
            }

            // read data from buffer half-0
            int realContentSize = squeeze();
            if (cacheIdx < realContentSize) {
                cacheIdx++;
                return buffer[cacheIdx];
            } else {
                ready0.set(0);
                halfReading.set(1);
                halfHaveConsumed.addAndGet(1);
                cacheIdx = 0;
                return -1;
            }
        } else {
            while (!(ready1.get() == concurrentStreams)) {
                LOG.warn("waiting for fetching oss data.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn("Something wrong, keep waiting.");
                }
            }

            // read data from buffer half-1
            int realContentSize = squeeze();
            if (cacheIdx < realContentSize) {
                cacheIdx++;
                return buffer[bufferSize/2+cacheIdx];
            } else {
                ready1.set(0);
                halfReading.set(0);
                halfHaveConsumed.addAndGet(1);
                cacheIdx = 0;
                return -1;
            }
        }
    }

    public synchronized int read(byte[] b, int off, int len) {
        if (halfReading.get() == 0) {
            while (!(ready0.get() == concurrentStreams)) {
                LOG.warn("waiting for fetching oss data.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn("Something wrong, keep waiting.");
                }
            }

            // read data from buffer half-0
            int size = 0;
            int realContentSize = squeeze();
            if (cacheIdx < splitContentSize[0]) {
                cacheIdx++;
                for (int i=0; i<len && cacheIdx<realContentSize; i++) {
                    b[off+i] = buffer[cacheIdx];
                    cacheIdx++;
                    size++;
                }
                return size;
            } else {
                ready0.set(0);
                halfReading.set(1);
                halfHaveConsumed.addAndGet(1);
                cacheIdx = 0;
                return -1;
            }
        } else {
            while (!(ready1.get() == concurrentStreams)) {
                LOG.warn("waiting for fetching oss data.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.warn("Something wrong, keep waiting.");
                }
            }

            // read data from buffer half-1
            int size = 0;
            int realContentSize = squeeze();
            if (cacheIdx < splitContentSize[0]) {
                cacheIdx++;
                for (int i=0; i<len && cacheIdx<realContentSize; i++) {
                    b[off+i] = buffer[cacheIdx];
                    cacheIdx++;
                    size++;
                }
                return size;
            } else {
                ready1.set(0);
                halfReading.set(0);
                halfHaveConsumed.addAndGet(1);
                cacheIdx = 0;
                return -1;
            }
        }
    }

    private int squeeze() {
        int totalSize = -1;
        for(int i=0; i<concurrentStreams; i++) {
            totalSize += splitContentSize[i];
        }
        int begin;
        if (halfReading.get() == 0) {
            begin = 0;
        } else {
            begin = bufferSize / 2;
        }
        int cacheIdx;
        if (totalSize != bufferSize) {
            cacheIdx = splitContentSize[0];
            for(int i=1; i <concurrentStreams; i++) {
                for (int j=0; j<splitContentSize[i]; j++) {
                    buffer[begin+cacheIdx] = buffer[begin+splitSize*i+j];
                    cacheIdx++;
                }
            }
        }

        return totalSize;
    }

    private class ConcurrentReader extends Task {
        private final Log LOG = LogFactory.getLog(ConcurrentReader.class);
        private Boolean preread = true;
        private int readerId = -1;
        private boolean half0Completed = false;
        private boolean half1Completed = false;
        private int half0StartPos = -1;
        private int half1StartPos = -1;
        private int length = -1;

        public ConcurrentReader(int readerId) {
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
            while (closed) {
                if (preread) {
                    preread = false;
                    // fetch oss data for half-0 and half-1 at the first time, as there is no data in buffer.
                    fetchData(half0StartPos);
                    half0Completed = true;
                    half1Completed = false;
                    ready0.addAndGet(1);
                }

                if (halfReading.get() == 0 && !half1Completed) {
                    // fetch oss data for half-1
                    fetchData(half1StartPos);
                    half1Completed = true;
                    half0Completed = false;
                    ready1.addAndGet(1);
                } else if (halfReading.get() == 1 && !half0Completed) {
                    // fetch oss data for half-0
                    fetchData(half0StartPos);
                    half0Completed = true;
                    half1Completed = false;
                    ready0.addAndGet(1);
                } else {
                    // waiting for `halfReading` block data to be consumed
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong when sleep.", e);
                    }
                }
            }
        }

        private void fetchData(int startPos) throws IOException {
            if (startPos == half0StartPos) {
                splitContentSize[readerId] = 0;
            } else {
                splitContentSize[concurrentStreams + readerId] = 0;
            }
            // fetch oss data for half-1
            int newpos = halfHaveConsumed.get() * bufferSize / 2 + half0StartPos;
            InputStream in = null;
            try {
                in = store.retrieve(key, newpos, length);
            } catch (Exception e) {
                throw new EOFException("Cannot open oss input stream");
            }

            int off = startPos;
            int tries = 10;
            int result;
            boolean retry = true;
            do {
                try {
                    result = in.read(buffer, off, length-off);
                    if (result > 0) {
                        off += result;
                    } else if (result == -1) {
                        break;
                    }
                    retry = off < length;
                } catch (EOFException e0) {
                    throw e0;
                } catch (Exception e1) {
                    tries--;
                    if (tries == 0) {
                        throw new IOException(e1);
                    }

                    LOG.warn("Some exceptions occurred in oss connection, try to reopen oss connection.", e1);
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
                        in = store.retrieve(key, newpos, length);
                    } catch (Exception e) {
                        throw new EOFException("Cannot open oss input stream");
                    }
                    off = startPos;
                }
            } while (tries>0 && retry);
            in.close();
            if (startPos == half0StartPos) {
                splitContentSize[readerId] = off;
            } else {
                splitContentSize[concurrentStreams + readerId] = off;
            }
        }
    }
}
