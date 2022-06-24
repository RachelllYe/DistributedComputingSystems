package cn.edu.ecnu.mapreduce.example.java.joinoperator;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceJoinWritable implements Writable {
    private String data;
    private String tag;

    public static final String Table1 =  "1";
    public static final String Table2 =  "2";
    ReduceJoinWritable(){}

    //注意读和写顺序一致
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tag);
        dataOutput.writeUTF(data);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        tag = dataInput.readUTF();
        data = dataInput.readUTF();
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getTag(){
        return tag;
    }
}
