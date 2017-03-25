package task;

/**
 * 设备信息 类
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class DevInfo {

	private int id; // 设备编号

	private String name; // 设备名称

	private String ip; // 主机IP

	private String hostUser; // 登陆用户名

	private String hostPwd; // 登陆密码

	private String hostSign; // 登陆提示符号

	private int omcID; // OMCID

	private int cityID; // 城市编码 by xumg

	private String vendor; // 厂商编号

	private String encode; // chensj 2010-8-6

	// 指定设备的编码，通常是用于FTP采集，需要知道FTP服务器的编码，以便处理中文路径或文件名

	public DevInfo() {
		super();
	}

	public int getID() {
		return this.id;
	}

	public void setID(int id) {
		this.id = id;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIP() {
		return this.ip;
	}

	public void setIP(String ip) {
		this.ip = ip;
	}

	public String getHostUser() {
		return this.hostUser;
	}

	public void setHostUser(String user) {
		this.hostUser = user;
	}

	public String getHostPwd() {
		return this.hostPwd;
	}

	public void setHostPwd(String pwd) {
		this.hostPwd = pwd;
	}

	public String getHostSign() {
		return this.hostSign;
	}

	public void setHostSign(String sign) {
		this.hostSign = sign;
	}

	public void setOmcID(int id) {
		this.omcID = id;
	}

	public int getOmcID() {
		return this.omcID;
	}

	public void setCityID(int cityID) {
		this.cityID = cityID;
	}

	public int getCityID() {
		return this.cityID;
	}

	public String getVendor() {
		return vendor;
	}

	public void setVendor(String vendor) {
		this.vendor = vendor;
	}

	public String getEncode() {
		return encode;
	}

	public void setEncode(String encode) {
		this.encode = encode;
	}

}
