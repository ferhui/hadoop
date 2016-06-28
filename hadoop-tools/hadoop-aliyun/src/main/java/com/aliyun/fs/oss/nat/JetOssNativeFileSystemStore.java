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

import com.aliyun.fs.oss.utils.OSSClientAgent;
import com.aliyun.fs.oss.utils.Utils;
import com.aliyun.fs.oss.utils.task.OSSPutTask;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import com.aliyun.fs.oss.common.*;
import com.aliyun.fs.oss.utils.task.OSSCopyTask;
import com.aliyun.fs.oss.utils.Result;
import com.aliyun.fs.oss.utils.task.Task;
import com.aliyun.fs.oss.utils.TaskEngine;
import org.apache.hadoop.mapreduce.SerializableETag;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JetOssNativeFileSystemStore implements NativeFileSystemStore {
    public static final Log LOG = LogFactory.getLog(JetOssNativeFileSystemStore.class);
    public static final String MULTIPART_UPLOAD_SUFFIX = ".upload";
    private int numSplitsUpperLimit = 10000;
    private Long maxSimpleCopySize;
    private Long maxSimplePutSize;

    private Configuration conf;

    private OSSClientAgent ossClientAgent;
    private String bucket;
    private int numCopyThreads;
    private int numPutThreads;
    private long maxSplitSize;
    private int numSplits;

    private String endpoint = null;
    private String accessKeyId = null;
    private String accessKeySecret = null;
    private String securityToken = null;

    public void initialize(URI uri, Configuration conf) throws Exception {
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Invalid hostname in URI " + uri);
        }
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] ossCredentials  = userInfo.split(":");
            if (ossCredentials.length >= 2) {
                accessKeyId = ossCredentials[0];
                accessKeySecret = ossCredentials[1];
            }
            if (ossCredentials.length == 3) {
                securityToken = ossCredentials[2];
            }
        }

        this.conf = conf;
        String host = uri.getHost();
        if (!StringUtils.isEmpty(host) && !host.contains(".")) {
            bucket = host;
        } else if (!StringUtils.isEmpty(host)) {
            bucket = host.substring(0, host.indexOf("."));
            endpoint = host.substring(host.indexOf(".") + 1);
        }

        if (accessKeyId == null) {
            accessKeyId = conf.getTrimmed("fs.oss.accessKeyId");
        }
        if (accessKeySecret == null) {
            accessKeySecret = conf.getTrimmed("fs.oss.accessKeySecret");
        }
        if (securityToken == null) {
            securityToken = conf.getTrimmed("fs.oss.securityToken");
        }

        if (endpoint == null) {
            endpoint = conf.getTrimmed("fs.oss.endpoint");
        }

        if (securityToken == null) {
            this.ossClientAgent = new OSSClientAgent(endpoint, accessKeyId, accessKeySecret, conf);
        } else {
            this.ossClientAgent = new OSSClientAgent(endpoint, accessKeyId, accessKeySecret, securityToken, conf);
        }

        this.numCopyThreads = conf.getInt("fs.oss.uploadPartCopy.thread.number", 10);
        this.numPutThreads = conf.getInt("fs.oss.uploadPart.thread.number", 5);
        this.maxSplitSize = conf.getLong("fs.oss.multipart.split.max.byte", 5 * 1024 * 1024L);
        this.numSplits = conf.getInt("fs.oss.multipart.split.number", 10);
        this.maxSimpleCopySize = conf.getLong("fs.oss.copy.simple.max.byte", 64 * 1024 * 1024L);
        this.maxSimplePutSize = conf.getLong("fs.oss.put.simple.max.byte", 64 * 1024 * 1024);
    }

    public List<PartETag> multiPartFile(String finalDstKey, File file, String uploadId) throws IOException {
        Long contentLength = file.length();
        Long minSplitSize = contentLength / numSplitsUpperLimit + 1;
        Long partSize = Math.max(Math.min(maxSplitSize, contentLength / numSplits), minSplitSize);
        int partCount = (int) (contentLength / partSize);
        if (contentLength % partSize != 0) {
            partCount++;
        }
        LOG.info("multipart uploading, partCount " + partCount + ", partSize " + partSize);

        List<PartETag> partETags = new ArrayList<PartETag>();
        List<Task> tasks = new ArrayList<Task>();
        for(int i=0; i<partCount; i++) {
            FileInputStream fis = new FileInputStream(file);
            long skipBytes = maxSplitSize * i;
            fis.skip(skipBytes);
            long size = maxSplitSize < contentLength - skipBytes ? maxSplitSize : contentLength - skipBytes;
            OSSPutTask ossPutTask = new OSSPutTask(ossClientAgent, uploadId, bucket, finalDstKey, size, skipBytes, i+1, file, conf);
            ossPutTask.setUuid(i+"");
            tasks.add(ossPutTask);
        }

        TaskEngine taskEngine = new TaskEngine(tasks, numPutThreads, numPutThreads);
        try {
            taskEngine.executeTask();
            Map<String, Object> responseMap = taskEngine.getResultMap();
            for(int i=0; i<partCount; i++) {
                UploadPartResult uploadPartResult = (UploadPartResult)
                        ((Result) responseMap.get(i+"")).getModels().get("uploadPartResult");
                partETags.add(uploadPartResult.getPartETag());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            taskEngine.shutdown();
        }

        return partETags;
    }

    public List<PartETag> multiPartFile(String finalDstKey, List<File> files, String uploadId) throws IOException {
        Long totalContentLength = 0L;
        for(File file: files) {
            totalContentLength += file.length();
        }
        Long minSplitSize = totalContentLength / numSplitsUpperLimit + 1;
        Long partSize = Math.max(Math.min(maxSplitSize, totalContentLength / numSplits), minSplitSize);
        int partCount = (int) (totalContentLength / partSize);
        if (totalContentLength % partSize != 0) {
            partCount++;
        }
        LOG.info("multipart uploading, partCount" + partCount + ", partSize " + partSize);

        List<Task> tasks = new ArrayList<Task>();
        int t = 0;
        for(File file: files) {
            Long contentLength = file.length();
            boolean _continue;
            int j = 0;
            long skipBytes;
            long size;
            do{
                skipBytes = partSize * j;
                if(partSize < contentLength - skipBytes) {
                    if((contentLength - (skipBytes + partSize)) < 1024 * 1024) {
                        size = contentLength - skipBytes;
                        _continue = false;
                    } else {
                        size = partSize;
                        _continue = true;
                    }
                } else {
                    size = contentLength - skipBytes;
                    _continue = false;
                }
                OSSPutTask ossPutTask =
                        new OSSPutTask(ossClientAgent, uploadId, bucket, finalDstKey, size, skipBytes, t+1, file, conf);
                ossPutTask.setUuid(t+"");
                tasks.add(ossPutTask);
                j++;
                t++;
            } while(_continue);
        }

        int realPartCount = tasks.size();
        List<PartETag> partETags = new ArrayList<PartETag>();
        TaskEngine taskEngine = new TaskEngine(tasks, numPutThreads, numPutThreads);
        try {
            taskEngine.executeTask();
            Map<String, Object> responseMap = taskEngine.getResultMap();
            for (int i = 0; i < realPartCount; i++) {
                UploadPartResult uploadPartResult = (UploadPartResult)
                        ((Result) responseMap.get(i+"")).getModels().get("uploadPartResult");
                partETags.add(uploadPartResult.getPartETag());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            taskEngine.shutdown();
        }

        return partETags;
    }

    @Override
    public void preStoreUploadId(String key, String finalDstKey, String uploadId) throws IOException {
        LOG.info("pre-store uploadId before uploading any file");
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bao);

        try {
            oos.writeObject(bucket);
            oos.writeObject(finalDstKey);
            oos.writeObject(uploadId);

            String uploadKey = key + MULTIPART_UPLOAD_SUFFIX;
            byte[] bytes = bao.toByteArray();
            ByteArrayInputStream bai = new ByteArrayInputStream(bytes);
            File dir = Utils.getTempBufferDir(conf);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException("Cannot create OSS buffer directory: " + dir);
            }
            File file = File.createTempFile("input-", ".uploadId", dir);
            FileOutputStream fos = new FileOutputStream(file);
            int byteCount;
            byte[] buffer = new byte[1024];
            while((byteCount = bai.read(buffer)) != -1) {
                fos.write(buffer, 0, byteCount);
            }
            fos.flush();
            fos.close();
            bai.close();
            ossClientAgent.putObject(bucket, uploadKey, file);
            file.delete();
        } catch (Exception e) {
            LOG.error("Some error occurs during pre-storing uploadId, " +
                    "uploadId "+uploadId+", bucket "+bucket+", key "+finalDstKey, e);
            throw new IOException(e);
        } finally {
            oos.flush();
            oos.close();
        }
    }

    @Override
    public void storeUploadIdAndPartETag(String key, String finalDstKey, List<PartETag> partETags, String uploadId)
            throws IOException {
        LOG.info("store uploadId and partETag after completing uploading file");
        List<SerializableETag> serializableETags = new ArrayList<SerializableETag>();
        for(PartETag partETag: partETags) {
            serializableETags.add(new SerializableETag().fromPartETag(partETag));
        }
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bao);
        try {
            oos.writeObject(bucket);
            oos.writeObject(finalDstKey);
            oos.writeObject(uploadId);
            oos.writeObject(serializableETags);

            String uploadKey = key + MULTIPART_UPLOAD_SUFFIX;
            byte[] bytes = bao.toByteArray();
            ByteArrayInputStream bai = new ByteArrayInputStream(bytes);
            File dir = Utils.getTempBufferDir(conf);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException("Cannot create OSS buffer directory: " + dir);
            }
            File file = File.createTempFile("input-", ".uploadId", dir);
            FileOutputStream fos = new FileOutputStream(file);
            int byteCount;
            byte[] buffer = new byte[1024];
            while((byteCount = bai.read(buffer)) != -1) {
                fos.write(buffer, 0, byteCount);
            }
            fos.flush();
            fos.close();
            bai.close();
            ossClientAgent.putObject(bucket, uploadKey, file);
            file.delete();
        } catch (Exception e) {
            LOG.error("Some error occurs during storing uploadId and partETag, " +
                    "uploadId "+uploadId+", bucket "+bucket+", key "+finalDstKey, e);
            throw new IOException(e);
        } finally {
            oos.flush();
            oos.close();
        }
    }

    public void storeFile(String key, File file, boolean append)
            throws IOException {
        try {
            if (!append) {
                Long fileLength = file.length();
                if (fileLength < Math.min(maxSimplePutSize, 512 * 1024 * 1024L)) {
                    ossClientAgent.putObject(bucket, key, file);
                } else {
                    // in some cases, we can not get finalOutputPath properly.
                    // 1. hadoop DistCp: the 'OUTDIR' is useless, and the procedure is different from a normal MR job.
                    // Each mapper do the 'copy' independently, and set the target path as Mapper arguments.
                    // 2. hadoop fs -cp: get 'OUTDIR' null
                    boolean redirectOutput = conf.getBoolean("job.output.oss.redirect", true);
                    String finalOutputPath = conf.get(FileOutputFormat.OUTDIR);
                    // compatible with old hadoop configuration
                    if (finalOutputPath == null || finalOutputPath.isEmpty()) {
                        finalOutputPath = conf.get("mapred.output.dir");
                    }

                    if (redirectOutput && finalOutputPath!=null) {
                        String finalDstPath = finalOutputPath + "/" + key.substring(key.lastIndexOf("/"));
                        String finalDstKey = NativeOssFileSystem.pathToKey(new Path(finalDstPath));
                        InitiateMultipartUploadResult initiateMultipartUploadResult =
                                ossClientAgent.initiateMultipartUpload(bucket, finalDstKey, conf);
                        String uploadId = initiateMultipartUploadResult.getUploadId();
                        preStoreUploadId(key, finalDstKey, uploadId);
                        List<PartETag> partETags = multiPartFile(finalDstKey, file, uploadId);
                        storeUploadIdAndPartETag(key, finalDstKey, partETags, uploadId);
                    } else {
                        InitiateMultipartUploadResult initiateMultipartUploadResult =
                                ossClientAgent.initiateMultipartUpload(bucket, key, conf);
                        String uploadId = initiateMultipartUploadResult.getUploadId();
                        List<PartETag> partETags = multiPartFile(key, file, uploadId);
                        ossClientAgent.completeMultipartUpload(bucket, key, uploadId, partETags, conf);
                    }
                }
            } else {
               throw new IOException("'append' op not supported.");
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public void storeFiles(String key, List<File> files, boolean append) throws IOException {
        try {
            if (!append) {
                if (files.size() == 1 && files.get(0).length() < Math.min(maxSimplePutSize, 512 * 1024 * 1024L)) {
                    ossClientAgent.putObject(bucket, key, files.get(0));
                } else {
                    StringBuilder sb = new StringBuilder();
                    for (File file : files) {
                        sb.append(file.getPath()).append(",");
                    }
                    int length = sb.toString().length();
                    sb.deleteCharAt(length - 1);
                    LOG.info("using multipart upload for key " + key + ", block files: " + sb.toString());

                    // in some cases, we can not get finalOutputPath properly.
                    // 1. hadoop DistCp: the 'OUTDIR' is useless, and the procedure is different from a normal MR job.
                    // Each mapper do the 'copy' independently, and set the target path as Mapper arguments.
                    // 2. hadoop fs -cp: get 'OUTDIR' null
                    boolean redirectOutput = conf.getBoolean("job.output.oss.redirect", true);
                    String finalOutputPath = conf.get(FileOutputFormat.OUTDIR);
                    if (redirectOutput && finalOutputPath!=null) {
                        String finalDstPath = finalOutputPath + "/" + key.substring(key.lastIndexOf("/"));
                        String finalDstKey = NativeOssFileSystem.pathToKey(new Path(finalDstPath));
                        InitiateMultipartUploadResult initiateMultipartUploadResult =
                                ossClientAgent.initiateMultipartUpload(bucket, finalDstKey, conf);
                        String uploadId = initiateMultipartUploadResult.getUploadId();
                        preStoreUploadId(key, finalDstKey, uploadId);
                        List<PartETag> partETags = multiPartFile(finalDstKey, files, uploadId);
                        storeUploadIdAndPartETag(key, finalDstKey, partETags, uploadId);
                    } else {
                        InitiateMultipartUploadResult initiateMultipartUploadResult =
                                ossClientAgent.initiateMultipartUpload(bucket, key, conf);
                        String uploadId = initiateMultipartUploadResult.getUploadId();
                        List<PartETag> partETags = multiPartFile(key, files, uploadId);
                        ossClientAgent.completeMultipartUpload(bucket, key, uploadId, partETags, conf);
                    }
                }
            } else {
                throw new IOException("'append' op not supported.");
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    public void storeEmptyFile(String key) throws IOException {
        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            objMeta.setContentLength(0);
            File dir = Utils.getTempBufferDir(conf);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException("Cannot create OSS buffer directory: " + dir);
            }
            File file = File.createTempFile("input-", ".empty", dir);
            ossClientAgent.putObject(bucket, key, file);
            file.delete();
        } catch (Exception e) {
            handleException(e);
        }
    }

    public FileMetadata retrieveMetadata(String key) throws IOException {
        try {
            if (!doesObjectExist(key)) {
                return null;
            }
            ObjectMetadata objectMetadata = ossClientAgent.getObjectMetadata(bucket, key);
            return new FileMetadata(key, objectMetadata.getContentLength(),
                    objectMetadata.getLastModified().getTime());
        } catch (Exception e) {
            // Following is brittle. Is there a better way?
            if (e.getMessage().contains("ResponseCode=404")) {
                return null;
            }
            return null; //never returned - keep compiler happy
        }
    }

    public InputStream retrieve(String key) throws IOException {
        try {
            if (!doesObjectExist(key)) {
                return null;
            }
            ObjectMetadata objectMetadata = ossClientAgent.getObjectMetadata(bucket, key);
            OSSObject object = ossClientAgent.getObject(bucket, key, 0, objectMetadata.getContentLength()-1, conf);
            return object.getObjectContent();
        } catch (Exception e) {
            handleException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    public InputStream retrieve(String key, long byteRangeStart)
            throws IOException {
        try {
            if (!doesObjectExist(key)) {
                return null;
            }
            ObjectMetadata objectMetadata = ossClientAgent.getObjectMetadata(bucket, key);
            long fileSize = objectMetadata.getContentLength();
            OSSObject object = ossClientAgent.getObject(bucket, key, byteRangeStart, fileSize-1, conf);
            return object.getObjectContent();
        } catch (Exception e) {
            handleException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    @Override
    public InputStream retrieve(String key, long byteRangeStart, long length) throws IOException {
        try {
            if (!doesObjectExist(key)) {
                return null;
            }
            ObjectMetadata objectMetadata = ossClientAgent.getObjectMetadata(bucket, key);
            long fileSize = objectMetadata.getContentLength();
            long end;
            if (fileSize-1 >= (byteRangeStart+length-1)) {
                end = byteRangeStart+length-1;
            } else {
                end = fileSize-1;
            }

            OSSObject object = ossClientAgent.getObject(bucket, key, byteRangeStart, end, conf);
            return object.getObjectContent();
        } catch (Exception e) {
            handleException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {
        return list(prefix, maxListingLength, null, false);
    }

    public PartialListing list(String prefix, int maxListingLength, String priorLastKey,
                               boolean recurse) throws IOException {

        return list(prefix, recurse ? null : NativeOssFileSystem.PATH_DELIMITER, maxListingLength, priorLastKey);
    }


    private PartialListing list(String prefix, String delimiter,
                                int maxListingLength, String priorLastKey) throws IOException {
        try {
            if (prefix.length() > 0 && !prefix.endsWith(NativeOssFileSystem.PATH_DELIMITER)) {
                prefix += NativeOssFileSystem.PATH_DELIMITER;
            }

            ObjectListing listing = ossClientAgent.listObjects(bucket, prefix, delimiter, maxListingLength, priorLastKey, conf);
            List<OSSObjectSummary> objects = listing.getObjectSummaries();

            FileMetadata[] fileMetadata =
                    new FileMetadata[objects.size()];
            Iterator<OSSObjectSummary> iter = objects.iterator();
            int idx = 0;
            while(iter.hasNext()) {
                OSSObjectSummary obj = iter.next();
                fileMetadata[idx] = new FileMetadata(obj.getKey(),
                        obj.getSize(), obj.getLastModified().getTime());
                idx += 1;
            }
            return new PartialListing(listing.getNextMarker(), fileMetadata, listing.getCommonPrefixes().toArray(new String[0]));
        } catch (Exception e) {
            handleException(e);
            return null; //never returned - keep compiler happy
        }
    }

    public void delete(String key) throws IOException {
        try {
            ossClientAgent.deleteObject(bucket, key);
        } catch (Exception e) {
            handleException(key, e);
        }
    }

    public void copy(String srcKey, String dstKey) throws IOException {
        try {
            if (!doesObjectExist(srcKey)) {
                return;
            }
            ObjectMetadata objectMetadata = ossClientAgent.getObjectMetadata(bucket, srcKey);
            Long contentLength = objectMetadata.getContentLength();
            if (contentLength <= Math.min(maxSimpleCopySize, 512 * 1024 * 1024L)) {
                ossClientAgent.copyObject(bucket, srcKey, bucket, dstKey);
            } else {
                InitiateMultipartUploadResult initiateMultipartUploadResult =
                        ossClientAgent.initiateMultipartUpload(bucket, dstKey, conf);
                String uploadId = initiateMultipartUploadResult.getUploadId();
                Long partSize = Math.min(maxSplitSize, contentLength / numSplits);
                int partCount = (int) (contentLength / partSize);
                if (contentLength % partSize != 0) {
                    partCount++;
                }
                List<PartETag> partETags = new ArrayList<PartETag>();
                List<Task> tasks = new ArrayList<Task>();
                for (int i = 0; i < partCount; i++) {
                    long skipBytes = partSize * i;
                    long size = partSize < contentLength - skipBytes ? partSize : contentLength - skipBytes;
                    OSSCopyTask ossCopyTask = new OSSCopyTask(
                            ossClientAgent, uploadId, bucket, bucket, srcKey, dstKey, size, skipBytes, i+1, conf);
                    ossCopyTask.setUuid(i+"");
                    tasks.add(ossCopyTask);
                }
                TaskEngine taskEngine = new TaskEngine(tasks, numCopyThreads, numCopyThreads);
                try {
                    taskEngine.executeTask();
                    Map<String, Object> responseMap = taskEngine.getResultMap();
                    for (int i = 0; i < partCount; i++) {
                        UploadPartCopyResult uploadPartCopyResult = (UploadPartCopyResult)
                                ((Result) responseMap.get(i+"")).getModels().get("uploadPartCopyResult");
                        partETags.add(uploadPartCopyResult.getPartETag());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    taskEngine.shutdown();
                }

                ossClientAgent.completeMultipartUpload(bucket, dstKey, uploadId, partETags, conf);
            }
        } catch (Exception e) {
            handleException(srcKey, e);
        }
    }

    @Override
    public String getUploadId(String dstKey) throws IOException {
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClientAgent.initiateMultipartUpload(bucket, dstKey, conf);
        return initiateMultipartUploadResult.getUploadId();
    }

    @Override
    public Task createOSSPutTask(File file, String finalDstKey, String uploadId, int idx) throws IOException {
        long contentLength = file.length();
        return new OSSPutTask(ossClientAgent, uploadId, bucket, finalDstKey, contentLength, 0L, idx, file, conf);
    }

    @Override
    public void completeUpload(String key, String uploadId, List<PartETag> partETags) throws IOException {
        ossClientAgent.completeMultipartUpload(bucket, key, uploadId, partETags, conf);
    }

    public void purge(String prefix) throws IOException {
        try {
            List<OSSObjectSummary> objects = ossClientAgent.listObjects(bucket, prefix).getObjectSummaries();
            for(OSSObjectSummary ossObjectSummary: objects) {
                ossClientAgent.deleteObject(bucket, ossObjectSummary.getKey());
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("OSS Native Filesystem, ");
        sb.append(bucket).append("\n");
        try {
            List<OSSObjectSummary> objects = ossClientAgent.listObjects(bucket).getObjectSummaries();
            for(OSSObjectSummary ossObjectSummary: objects) {
                sb.append(ossObjectSummary.getKey()).append("\n");
            }
        } catch (Exception e) {
            handleException(e);
        }
        System.out.println(sb);
    }

    private void handleException(String key, Exception e) throws IOException, OssException {
        if (e instanceof ServiceException && "NoSuchKey".equals(((ServiceException) e).getErrorCode())) {
            throw new FileNotFoundException("Key '" + key + "' does not exist in OSS");
        } else {
            handleException(e);
        }
    }

    private void handleException(Exception e) throws IOException, OssException {
        if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        }
        else {
            throw new OssException(e);
        }
    }

    private boolean doesObjectExist(String key) {
        try {
            return ossClientAgent.doesObjectExist(bucket, key);
        } catch (Exception e) {
            return false;
        }
    }
}
