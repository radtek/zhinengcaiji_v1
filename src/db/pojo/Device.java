package db.pojo;

/**
 * 模板POJO
 * <P>
 * 对应IGP_CONF_DEVICE表一条记录
 * </p>
 * 
 * @author yuanxf
 * @since 1.0
 */
public class Device {

	private int devID;        // 设备ID

	private String devName;   // 设备名称

	private int cityID;       // 城市ID

	private int omcID;        // OMC ID

	private String vendor;    // 厂商

	private String hostIP;    // 主机IP地址

	private String hostUser;  // 登录用户名

	private String hostPwd;   // 登录密码

	private String hostSign;  // 登录提示符

	public int getDevID() {
		return devID;
	}

	public void setDevID(int devID) {
		this.devID = devID;
	}

	public String getDevName() {
		return devName;
	}

	public void setDevName(String devName) {
		this.devName = devName;
	}

	public int getCityID() {
		return cityID;
	}

	public void setCityID(int cityID) {
		this.cityID = cityID;
	}

	public int getOmcID() {
		return omcID;
	}

	public void setOmcID(int omcID) {
		this.omcID = omcID;
	}

	public String getVendor() {
		return vendor;
	}

	public void setVendor(String vendor) {
		this.vendor = vendor;
	}

	public String getHostIP() {
		return hostIP;
	}

	public void setHostIP(String hostIP) {
		this.hostIP = hostIP;
	}

	public String getHostUser() {
		return hostUser;
	}

	public void setHostUser(String hostUser) {
		this.hostUser = hostUser;
	}

	public String getHostPwd() {
		return hostPwd;
	}

	public void setHostPwd(String hostPwd) {
		this.hostPwd = hostPwd;
	}

	public String getHostSign() {
		return hostSign;
	}

	public void setHostSign(String hostSign) {
		this.hostSign = hostSign;
	}

	public static void main(String[] args) {

	}

}
