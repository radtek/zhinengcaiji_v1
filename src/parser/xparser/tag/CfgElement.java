package parser.xparser.tag;

public class CfgElement extends Tag {

	private String charset;

	private String driver;

	private String url;

	private String username;

	private String password;

	private String service;

	private String sign;

	private int backlogCount;

	private int skip;

	public CfgElement() {
		super("cfg");
	}

	public CfgElement(String charset, String driver, String url, String username, String password, String service, String sign, int backlogCount,
			int skip) {
		this();
		this.charset = charset;
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
		this.service = service;
		this.sign = sign;
		this.backlogCount = backlogCount;
		this.skip = skip;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public int getBacklogCount() {
		return backlogCount;
	}

	public void setBacklogCount(int backlogCount) {
		this.backlogCount = backlogCount;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getDirver() {
		return driver;
	}

	public void setDirver(String dirver) {
		this.driver = dirver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public int getSkip() {
		return skip;
	}

	public void setSkip(int skip) {
		this.skip = skip;
	}

	@Override
	public Object apply(Object params) {
		// TODO Auto-generated method stub
		return null;
	}
}
