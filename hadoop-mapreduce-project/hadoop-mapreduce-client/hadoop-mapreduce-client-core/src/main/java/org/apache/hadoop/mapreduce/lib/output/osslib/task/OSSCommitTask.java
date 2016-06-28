package org.apache.hadoop.mapreduce.lib.output.osslib.task;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.SerializableETag;
import org.apache.hadoop.mapreduce.lib.output.osslib.OSSClientAgent;
import org.apache.hadoop.mapreduce.lib.output.osslib.Result;
import org.apache.hadoop.mapreduce.lib.output.osslib.TaskEngine;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class OSSCommitTask extends Task {
    private static final Log LOG = LogFactory.getLog(OSSCommitTask.class);

    private FileSystem fs;
    private OSSClientAgent ossClientAgent;
    private Path uploadIdFile;

    public OSSCommitTask(FileSystem fs,
                         OSSClientAgent ossClientAgent,
                         Path uploadIdFile) {
        this.fs = fs;
        this.ossClientAgent = ossClientAgent;
        this.uploadIdFile = uploadIdFile;
    }

    @Override
    public void execute(TaskEngine engineRef) {
        Result result = new Result();
        InputStream in;
        ObjectInputStream ois = null;
        String bucket;
        String finalDstKey = "";
        String uploadId = "";
        try {
            in = fs.open(uploadIdFile);
            ois = new ObjectInputStream(in);

            bucket = (String) ois.readObject();
            finalDstKey = (String) ois.readObject();
            uploadId = (String) ois.readObject();
            List<SerializableETag> serializableETags = (List<SerializableETag>) ois.readObject();
            List<PartETag> partETags = new ArrayList<PartETag>();
            for(SerializableETag serializableETag: serializableETags) {
                partETags.add(serializableETag.toPartETag());
            }
            CompleteMultipartUploadResult completeMultipartUploadResult =
                    ossClientAgent.completeMultipartUpload(bucket, finalDstKey, uploadId, partETags);
            fs.delete(uploadIdFile, true);
            LOG.info("complete multi-part upload " + uploadId +
                    ": [" + completeMultipartUploadResult.getETag() + "]");

            result.getModels().put("completeMultipartUploadResult", completeMultipartUploadResult);
            // TODO: fail?
            result.setSuccess(true);
            this.response = result;
        } catch (Exception e) {
            LOG.error("Failed to complete multi-part " + uploadId + ", key: " + finalDstKey, e);
            result.setSuccess(false);
            this.response = result;
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.error("Failed to close oss input stream");
                }
            }
        }
    }
}
