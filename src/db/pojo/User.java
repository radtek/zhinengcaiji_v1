package db.pojo;

/**
 * 用户POJO类 "IGP_CONF_USER"表
 * 
 * @author YangJian
 * @since 1.0
 */
public class User {

	private long id;

	private String userName;

	private String userPwd;

	private UserGroup group;

	public UserGroup getGroup() {
		return group;
	}

	public User() {
		super();
	}

	public User(int id, String userName, String userPwd, int groupID) {
		super();
		this.id = id;
		this.userName = userName;
		this.userPwd = userPwd;
		this.group.setId(groupID);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserPwd() {
		return userPwd;
	}

	public void setUserPwd(String userPwd) {
		this.userPwd = userPwd;
	}

	public int getGroupID() {
		return this.group.getId();
	}

	public void setGroupID(int groupID) {
		if (this.group == null)
			this.group = new UserGroup();
		this.group.setId(groupID);
	}

	public String getGroupName() {
		return this.group.getName();
	}

	public void setGroup(UserGroup group) {
		this.group = group;
	}
}
