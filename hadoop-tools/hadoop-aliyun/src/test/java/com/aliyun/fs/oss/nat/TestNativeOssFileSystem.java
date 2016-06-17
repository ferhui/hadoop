/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fs.oss.nat;

import com.aliyun.fs.oss.common.InMemoryNativeFileSystemStore;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class TestNativeOssFileSystem extends TestCase {
    private Configuration conf;
    private JetOssNativeFileSystemStore store;
    private NativeOssFileSystem fs;

    public void testInitialization() throws IOException {
        initializationTest("oss://a:b@c", "oss://a:b@c");
        initializationTest("oss://a:b@c/", "oss://a:b@c");
        initializationTest("oss://a:b@c/path", "oss://a:b@c");
        initializationTest("oss://a@c", "oss://a@c");
        initializationTest("oss://a@c/", "oss://a@c");
        initializationTest("oss://a@c/path", "oss://a@c");
        initializationTest("oss://c", "oss://c");
        initializationTest("oss://c/", "oss://c");
        initializationTest("oss://c/path", "oss://c");
    }

    private void initializationTest(String initializationUri, String expectedUri)
            throws IOException {
        NativeOssFileSystem fs = new NativeOssFileSystem(new InMemoryNativeFileSystemStore());
        fs.initialize(URI.create(initializationUri), conf);
        assertEquals(URI.create(expectedUri), fs.getUri());
    }

    private Path path(String pathString) {
        return new Path("oss://emr/" + pathString).makeQualified(fs);
    }

    @Override
    protected void setUp() throws IOException {
        conf = new Configuration();
        conf.set("job.runlocal", "true");
        conf.set("fs.oss.endpoint", "endpoint");
        conf.set("fs.oss.accessKeyId", "accessKeyId");
        conf.set("fs.oss.accessKeySecret", "accessKeySecret");
        store = new JetOssNativeFileSystemStore();
        fs = new NativeOssFileSystem(store);
        fs.initialize(URI.create("oss://emr/"), conf);
    }

    @Override
    protected void tearDown() throws Exception {
        store.purge("uttest");
        super.tearDown();
    }

    private void createTestFiles(String base) throws IOException {
        store.storeEmptyFile(base + "/file1");
        store.storeEmptyFile(base + "/dir/file2");
        store.storeEmptyFile(base + "/dir/file3");
    }

    public void testDeleteWithNoMarker() throws IOException {
        String base = "uttest/oss";
        Path path = path(base);

        createTestFiles(base);

        fs.delete(path, true);
        assert(fs.listStatus(path).length == 0);
    }

    public void testRenameWithNoMarker() throws IOException {
        String base = "uttest/oss";
        Path target = path("uttest/oss2");

        createTestFiles(base);

        fs.rename(path(base), target);

        Path path = path("uttest");
        assertTrue(fs.getFileStatus(path).isDir());
        assertEquals(1, fs.listStatus(path).length);
        assertTrue(fs.getFileStatus(target).isDir());
        assertEquals(2, fs.listStatus(target).length);
    }

    public void testEmptyFile() throws Exception {
        store.storeEmptyFile("uttest/oss/file1");
        fs.open(path("uttest/oss/file1")).close();
    }

    public void testRename() throws IOException {
        Path path = path("uttest/dir/file1");
        Path renamed = path("uttest/dir/file2");
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.write("Hello".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        fs.rename(path, renamed);
        assert(fs.exists(renamed));
        assert(!fs.exists(path));
    }

    public void testListStatus() throws IOException {
        String key0 = "uttest/file0";
        String key1 = "uttest/dir1/file1";
        String key2 = "uttest/dir1/dir12/file2";
        String key3 = "uttest/dir2/file3";
        store.storeEmptyFile(key0);
        store.storeEmptyFile(key1);
        store.storeEmptyFile(key2);
        store.storeEmptyFile(key3);

        assert(fs.listStatus(path("uttest")).length == 3);
        assert(fs.listStatus(path("uttest/file0")).length == 1);
        assert(fs.listStatus(path("uttest/dir1")).length == 2);
        assert(fs.listStatus(path("uttest/dir1/file1")).length == 1);
        assert(fs.listStatus(path("uttest/dir1/dir12/file2")).length == 1);
        assert(fs.listStatus(path("uttest/dir2")).length == 1);
        assert(fs.listStatus(path("uttest/dir2/file3")).length == 1);
        assert(fs.listStatus(path("uttest/dir3")).length == 0);

        assert(fs.listStatus(path("uttest/dir1/file1"))[0].isFile());
        assert(fs.listStatus(path("uttest/dir2"))[0].isFile());
        assert(fs.listStatus(path("uttest/dir1"))[0].isFile() && fs.listStatus(path("uttest/dir1"))[1].isDirectory()
                || fs.listStatus(path("uttest/dir1"))[1].isFile() && fs.listStatus(path("uttest/dir1"))[0].isDirectory());
    }

    public void testDelete() throws IOException {
        String key0 = "uttest/file0";
        String key1 = "uttest/dir1/file1";
        String key2 = "uttest/dir1/dir12/file2";
        store.storeEmptyFile(key0);
        store.storeEmptyFile(key1);
        store.storeEmptyFile(key2);

        assert(!fs.delete(path("test2")));
        try {
            fs.delete(path("uttest"), false);
            assert(1 == 0);
        } catch (Exception e) {
            assert(1 == 1);
        }

        assert(fs.delete(path("uttest/file0")));
        assert(fs.delete(path("uttest/dir1")));
    }

    public void testFileStatus() throws IOException {
        store.storeEmptyFile("uttest/dir1/file1");

        assert(fs.getFileStatus(path("uttest/dir1/file1")).isFile());
        assert(fs.getFileStatus(path("uttest/dir1")).isDirectory());
        assert(fs.getFileStatus(path("/")).isDirectory());
        try {
            fs.getFileStatus(path("test2"));
            assert(1 == 0);
        } catch (Exception e) {
            assert(1 == 1);
        }
    }

    public void testMkdirs() throws IOException {
        Path p1 = path("uttest/dir1");
        Path p2 = path("uttest/dir1/dir2");

        fs.mkdirs(p1);
        assert(fs.getFileStatus(p1).isDirectory());

        fs.mkdirs(p2);
        assert(fs.getFileStatus(p1).isDirectory());
        assert(fs.getFileStatus(p2).isDirectory());
    }
}
