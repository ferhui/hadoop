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

package com.aliyun.fs.oss.common;

import com.aliyun.fs.oss.utils.task.Task;
import com.aliyun.oss.model.PartETag;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
public interface NativeFileSystemStore {

    void initialize(URI uri, Configuration conf) throws Exception;

    void storeFile(String key, File file, boolean append) throws IOException;
    void storeFiles(String key, List<File> files, boolean append) throws IOException;
    void storeEmptyFile(String key) throws IOException;

    FileMetadata retrieveMetadata(String key) throws IOException;
    InputStream retrieve(String key) throws IOException;
    InputStream retrieve(String key, long byteRangeStart) throws IOException;
    InputStream retrieve(String key, long byteRangeStart, long length) throws IOException;

    PartialListing list(String prefix, int maxListingLength) throws IOException;
    PartialListing list(String prefix, int maxListingLength, String priorLastKey, boolean recursive)
            throws IOException;

    void delete(String key) throws IOException;
    void copy(String srcKey, String dstKey) throws IOException;
    void cleanup() throws Exception;

    String getUploadId(String dstKey) throws IOException;
    Task createOSSPutTask(File file, String finalDstKey, String uploadId, int idx) throws IOException;
    void completeUpload(String key, String uploadId, List<PartETag> partETags) throws IOException;
    void preStoreUploadId(String key, String finalDstKey, String uploadId) throws IOException;
    void storeUploadIdAndPartETag(String key, String finalDstKey, List<PartETag> partETags, String uploadId)
            throws IOException;
    /**
     * Delete all keys with the given prefix. Used for testing.
     * @throws IOException
     */
    void purge(String prefix) throws IOException;

    /**
     * Diagnostic method to dump state to the console.
     * @throws IOException
     */
    void dump() throws IOException;
}
