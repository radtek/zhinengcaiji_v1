package parser.hw.dt.bean;

/**
 * 用于记录每一个GPS_LINK_SEQ对应的经纬度
 * 
 * @author lijiayu
 * @date 2013年9月24日
 */
public class LongiLatitude{

	// 经度
	private double longitude;

	// 纬度
	private double latitude;

	public double getLongitude(){
		return longitude;
	}

	public void setLongitude(double longitude){
		this.longitude = longitude;
	}

	public double getLatitude(){
		return latitude;
	}

	public void setLatitude(double latitude){
		this.latitude = latitude;
	}

}
