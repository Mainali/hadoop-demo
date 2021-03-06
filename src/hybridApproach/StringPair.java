package hybridApproach;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable<StringPair> {
	Text key;
	Text neighbour;
	
	public StringPair(){
		this.key = new Text();
		this.neighbour = new Text();
	}
	
	
	public void setPairs(String k, String n){
		this.key.set(k);
		this.neighbour.set(n);
	}
	
	public Text getKey(){
		return this.key;
		
	}
	
	public Text getNeighbour(){
		return this.neighbour;
		
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		key.readFields(arg0);
		neighbour.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		key.write(arg0);
		neighbour.write(arg0);
	}

	@Override
	public int compareTo(StringPair o) {
		// TODO Auto-generated method stub
		if(o==null)
			return 0;
		
		int res = key.toString().compareTo(o.key.toString());
		return (res==0) ? neighbour.toString().compareTo(o.neighbour.toString()): res;
	}
	
	
	@Override 
	public String toString(){
		return "("+key.toString()+","+neighbour.toString()+")";
	}
	

}
