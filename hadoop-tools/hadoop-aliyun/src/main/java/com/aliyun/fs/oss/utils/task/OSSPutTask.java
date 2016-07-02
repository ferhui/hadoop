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
package com.aliyun.fs.oss.utils.task;

import com.aliyun.fs.oss.utils.OSSClientAgent;
import com.aliyun.fs.oss.utils.Result;
import com.aliyun.fs.oss.utils.TaskEngine;

import com.aliyun.oss.model.UploadPartResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.*;

public class OSSPutTask extends Task {
    private static final Log LOG = LogFactory.getLog(OSSPutTask.class);
    OSSClientAgent ossClientAgent;
    private String uploadId;
    private String bucket;
    private String key;
    private Long partSize;
    private Long beginIndex;
    private int partNumber;
    private File localFile;
    private Configuration conf;
    private boolean deleteAfterUse = false;

    public OSSPutTask(OSSClientAgent ossClientAgent,
                      String uploadId,
                      String bucket,
                      String key,
                      Long partSize,
                      Long beginIndex,
                      int partNumber,
                      File file,
                      Configuration conf,
                      boolean deleteAfterUse) {
        this.ossClientAgent = ossClientAgent;
        this.uploadId = uploadId;
        this.bucket = bucket;
        this.key = key;
        this.partSize = partSize;
        this.beginIndex = beginIndex;
        this.partNumber = partNumber;
        this.localFile = file;
        this.conf = conf;
        this.deleteAfterUse = deleteAfterUse;
    }

    public OSSPutTask(OSSClientAgent ossClientAgent,
                      String uploadId,
                      String bucket,
                      String key,
                      Long partSize,
                      Long beginIndex,
                      int partNumber,
                      File file,
                      Configuration conf) {
        this.ossClientAgent = ossClientAgent;
        this.uploadId = uploadId;
        this.bucket = bucket;
        this.key = key;
        this.partSize = partSize;
        this.beginIndex = beginIndex;
        this.partNumber = partNumber;
        this.localFile = file;
        this.conf = conf;
    }

    @Override
    public void execute(TaskEngine engineRef) {
        Result result = new Result();
        int tries = 3;
        while(tries > 0) {
            try {
                UploadPartResult uploadPartResult = ossClientAgent.uploadPart(uploadId, bucket, key, partSize, beginIndex,
                        partNumber, localFile, conf);
                result.getModels().put("uploadPartResult", uploadPartResult);
                // TODO: fail?
                if (deleteAfterUse) {
                    localFile.delete();
                }
                result.setSuccess(true);
                this.response = result;
                break;
            } catch (Exception e) {
                LOG.info("uploading " + key + ", partSize " + partSize + ", beginIndex " + beginIndex + ", partNumber " +
                        partNumber + ", localFile "  + localFile + ", file size " + localFile.length());
                LOG.error("Failed to upload oss file, try again.", e);
            }

            tries--;
        }

        if (tries == 0) {
            result.setSuccess(false);
            this.response = result;
        }
    }
}

