package com.aliyun.fs.oss.nat;

import com.aliyun.fs.oss.common.NativeFileSystemStore;
import com.aliyun.fs.oss.utils.TaskEngine;
import com.aliyun.fs.oss.utils.task.Task;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
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
    private AtomicInteger halfHaveConsumed = new AtomicInteger(1);
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
                while (!(ready0.get() == concurrentStreams)) {
                    LOG.warn("waiting for fetching oss data, ready0 has completed " + ready0.get());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                }
                if (!squeezed0) {
                    realContentSize = squeeze();
                    squeezed0 = true;
                    squeezed1 = false;
                }

                // read data from buffer half-0
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    cacheIdx++;
                    pos++;
                    return buffer[cacheIdx];
                } else {
                    ready0.set(0);
                    halfReading.set(1);
                    halfHaveConsumed.addAndGet(1);
                    cacheIdx = 0;
                }
            } else {
                while (!(ready1.get() == concurrentStreams)) {
                    LOG.warn("waiting for fetching oss data, ready1 has completed " + ready1.get());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                }
                if (!squeezed1) {
                    realContentSize = squeeze();
                    squeezed0 = false;
                    squeezed1 = true;
                }

                // read data from buffer half-1
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    cacheIdx++;
                    return buffer[bufferSize / 2 + cacheIdx];
                } else {
                    ready1.set(0);
                    halfReading.set(0);
                    halfHaveConsumed.addAndGet(1);
                    cacheIdx = 0;
                }
            }
        }
    }

    public synchronized int read(byte[] b, int off, int len) {
        while(true) {
            if (halfReading.get() == 0) {
                while (!(ready0.get() == concurrentStreams)) {
                    LOG.warn("waiting for fetching oss data, ready0 has completed " + ready0.get());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                }
                if (!squeezed0) {
                    realContentSize = squeeze();
                    squeezed0 = true;
                    squeezed1 = false;
                }

                // read data from buffer half-0
                int size = 0;
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    cacheIdx++;
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
                    halfHaveConsumed.addAndGet(1);
                    cacheIdx = 0;
                }
            } else {
                while (!(ready1.get() == concurrentStreams)) {
                    LOG.warn("waiting for fetching oss data, ready1 has completed " + ready1.get());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.warn("Something wrong, keep waiting.");
                    }
                }
                if (!squeezed1) {
                    realContentSize = squeeze();
                    squeezed0 = false;
                    squeezed1 = true;
                }

                // read data from buffer half-1
                int size = 0;
                if (pos >= fileContentLength) {
                    return -1;
                } else if (cacheIdx < realContentSize) {
                    cacheIdx++;
                    for (int i = 0; i < len && cacheIdx < realContentSize; i++) {
                        b[off + i] = buffer[cacheIdx];
                        cacheIdx++;
                        pos++;
                        size++;
                    }
                    return size;
                } else {
                    ready1.set(0);
                    halfReading.set(0);
                    halfHaveConsumed.addAndGet(1);
                    cacheIdx = 0;
                }
            }
        }
    }

    private int squeeze() {
        int totalSize = 0;
        for(int i=0; i<concurrentStreams; i++) {
            totalSize += splitContentSize[i];
            LOG.info("split content size " + i + " : " + splitContentSize[i]);
        }
        LOG.info("total size: " + totalSize);
        int begin;
        if (halfReading.get() == 0) {
            begin = 0;
        } else {
            begin = bufferSize / 2;
        }
        int cacheIdx;
        if (totalSize != bufferSize) {
            LOG.info("total size != bufferSize");
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
                if (preread) {
                    LOG.info("[ConcurrentReader-"+readerId+"] preread: " + preread);
                    // fetch oss data for half-0 and half-1 at the first time, as there is no data in buffer.
                    _continue = fetchData(half0StartPos);
                    half0Completed = true;
                    half1Completed = false;
                    ready0.addAndGet(1);
                    preread = false;
                } else if (halfReading.get() == 0 && !half1Completed) {
                    LOG.info("[ConcurrentReader-"+readerId+"] halfReading: " + halfReading.get());
                    // fetch oss data for half-1
                    _continue = fetchData(half1StartPos);
                    half1Completed = true;
                    half0Completed = false;
                    ready1.addAndGet(1);
                } else if (halfReading.get() == 1 && !half0Completed) {
                    LOG.info("[ConcurrentReader-"+readerId+"] halfReading: " + halfReading.get());
                    // fetch oss data for half-0
                    _continue = fetchData(half0StartPos);
                    half0Completed = true;
                    half1Completed = false;
                    ready0.addAndGet(1);
                } else {
                    // waiting for `halfReading` block data to be consumed
                    LOG.info("[ConcurrentReader-"+readerId+"] waiting for `halfReading` block data to be consumed");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.info("[ConcurrentReader-"+readerId+"] Something wrong when sleep, " + e.getMessage());
                    }
                }
            }
        }

        private boolean fetchData(int startPos) throws IOException {
            boolean ret = true;
            if (startPos == half0StartPos) {
                splitContentSize[readerId] = 0;
            } else {
                splitContentSize[concurrentStreams + readerId] = 0;
            }
            // fetch oss data for half-1
            long newpos = halfHaveConsumed.get() * bufferSize / 2 + half0StartPos;
            int fetchLength = length;
            if (preread && bufferSize >= fileContentLength) {
                ret = false;
                fetchLength = (int) fileContentLength / concurrentStreams;
                newpos = fetchLength * readerId;
                if (readerId == (concurrentStreams-1)) {
                    fetchLength = (int) fileContentLength - fetchLength * (concurrentStreams - 1);
                }
                LOG.info("[ConcurrentReader-"+readerId+"] 1 ----> fetchLength: " + fetchLength + ", newpos: " + newpos);
            } else if ((halfHaveConsumed.get()+1) * bufferSize >= fileContentLength) {
                ret = false;
                fetchLength = (int) (fileContentLength - halfHaveConsumed.get() * bufferSize) / concurrentStreams;
                newpos = halfHaveConsumed.get() * bufferSize + readerId * fetchLength;
                if (readerId == (concurrentStreams-1)) {
                    fetchLength = (int) fileContentLength - halfHaveConsumed.get() * bufferSize - (fetchLength * concurrentStreams - 1);
                }
                LOG.info("[ConcurrentReader-"+readerId+"] 2 ----> fetchLength: " + fetchLength + ", newpos: " + newpos);
            }
            InputStream in = null;
            try {
                LOG.info("[ConcurrentReader-"+readerId+"] key: " + key + ", new pos: " + newpos + ", fetchLength: " + fetchLength);
                in = store.retrieve(key, newpos, fetchLength);
            } catch (Exception e) {
                LOG.info("[ConcurrentReader-"+readerId+"]", e);
                throw new EOFException("[ConcurrentReader-"+readerId+"] Cannot open oss input stream");
            }

            int off = startPos;
            int tries = 10;
            int result;
            boolean retry = true;
            int hasReaded = 0;
            do {
                try {
                    result = in.read(buffer, off, fetchLength-hasReaded);
                    if (result > 0) {
                        off += result;
                        hasReaded += result;
                    } else if (result == -1) {
                        break;
                    }
                    retry = hasReaded < fetchLength;
                    LOG.info("[ConcurrentReader-"+readerId+"] fetch: " + result + ", hasreaded: " + hasReaded + ", off: " + off);
                } catch (EOFException e0) {
                    throw e0;
                } catch (Exception e1) {
                    tries--;
                    if (tries == 0) {
                        throw new IOException(e1);
                    }

                    LOG.info("[ConcurrentReader-"+readerId+"] Some exceptions occurred in oss connection, try to reopen oss connection", e1);
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
                        in = store.retrieve(key, newpos, fetchLength);
                    } catch (Exception e) {
                        throw new EOFException("[ConcurrentReader-"+readerId+"] Cannot open oss input stream");
                    }
                    off = startPos;
                    hasReaded = 0;
                }
                LOG.info("[ConcurrentReader-"+readerId+"] retry: " + retry + ", tries remain: " + tries + ", off: " + off +
                        ", newpos: " + newpos + ", fetchLength: " + fetchLength);
            } while (tries>0 && retry);
            in.close();
            if (startPos == half0StartPos) {
                LOG.info("splitContentSize " + readerId + ": " + hasReaded);
                splitContentSize[readerId] = hasReaded;
            } else {
                splitContentSize[concurrentStreams + readerId] = hasReaded;
            }

            return ret;
        }
    }
}
