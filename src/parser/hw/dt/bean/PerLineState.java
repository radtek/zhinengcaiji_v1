package parser.hw.dt.bean;

/**
 * 用于记录每一行的长度与时间
 * 
 * @author lijiayu @ 2013年9月24日
 */
public class PerLineState{

	// 当前数据的长度
	private int length;

	// 当前数据的时间
	private String time;

	private int sid;

	private int nid;

	private int pn;

	private int carr;

	public int getLength(){
		return length;
	}

	public void setLength(int length){
		this.length = length;
	}

	public String getTime(){
		return time;
	}

	public void setTime(String time){
		this.time = time;
	}

	public int getSid(){
		return sid;
	}

	public void setSid(int sid){
		this.sid = sid;
	}

	public int getNid(){
		return nid;
	}

	public void setNid(int nid){
		this.nid = nid;
	}

	public int getPn(){
		return pn;
	}

	public void setPn(int pn){
		this.pn = pn;
	}

	public int getCarr(){
		return carr;
	}

	public void setCarr(int carr){
		this.carr = carr;
	}

}
