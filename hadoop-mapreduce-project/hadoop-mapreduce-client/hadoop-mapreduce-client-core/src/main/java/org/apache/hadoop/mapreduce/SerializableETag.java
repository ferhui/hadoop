package org.apache.hadoop.mapreduce;

import com.aliyun.oss.model.PartETag;

import java.io.Serializable;

public class SerializableETag implements Serializable {
    private int partNumber;
    private String eTag;

    public SerializableETag() {

    }

    public SerializableETag(int partNumber, String eTag) {
        this.partNumber = partNumber;
        this.eTag = eTag;
    }

    public int getPartNumber() {
        return this.partNumber;
    }

    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    public String getETag() {
        return this.eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public SerializableETag fromPartETag(PartETag partETag) {
        this.eTag = partETag.getETag();
        this.partNumber = partETag.getPartNumber();
        return this;
    }

    public PartETag toPartETag() {
        return new PartETag(partNumber, eTag);
    }
}
