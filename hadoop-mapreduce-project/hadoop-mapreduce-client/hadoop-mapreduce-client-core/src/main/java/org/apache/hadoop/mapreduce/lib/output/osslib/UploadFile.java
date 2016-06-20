package org.apache.hadoop.mapreduce.lib.output.osslib;

import com.aliyun.oss.model.PartETag;

import java.util.List;

/**
 * Created by yugm on 6/19/16.
 */
public class UploadFile {
    String path;
    String bucket;
    String dstKey;
    String uploadId;
    List<PartETag> partETags;

    public UploadFile(String path) {
        this.path = path;
    }
}
