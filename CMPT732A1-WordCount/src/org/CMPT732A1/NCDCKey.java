package org.CMPT732A1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
 
public class NCDCKey implements WritableComparable{
 
    private Text first;
    private Text second;
    
 
    public NCDCKey(Text first, Text second) {
        set(first, second);
    }
 
    public NCDCKey() {
        set(new Text(), new Text());
    }
 
    public NCDCKey(String first, String second) {
        set(new Text(first), new Text(second));
    }
 
    public Text getFirst() {
        return first;
    }
 
    public Text getSecond() {
        return second;
    }
 
    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
 
    @Override
    public String toString() {
        return first + " " + second;
    }
 
    @Override
    public int compareTo( Object o) {
    	NCDCKey tp = (NCDCKey) o;
        int cmp = first.compareTo(tp.first);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return second.compareTo(tp.second);
    }
 
    @Override
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof NCDCKey)
        {
        	NCDCKey tp = (NCDCKey) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
 
}
