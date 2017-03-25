package db.pojo;

import java.util.List;

/**
 * 用户分组POJO "IGP_CONF_USERGROUP"表
 * 
 * @author YangJian
 * @since 1.0
 * @see User
 */
public class UserGroup {

	private int id;

	private String name;

	private String ids;

	private String note;

	private List<User> users;

	public UserGroup() {
		super();
	}

	public UserGroup(int id, String name, String ids, String note) {
		super();
		this.id = id;
		this.name = name;
		this.ids = ids;
		this.note = note;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIds() {
		return ids;
	}

	public void setIds(String ids) {
		this.ids = ids;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}

	public List<User> getUsers() {
		return users;
	}

	public void setUsers(List<User> users) {
		this.users = users;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof UserGroup))
			return false;
		UserGroup g = (UserGroup) obj;
		return g.getId() == this.id;
	}
	
	@Override
	public int hashCode(){
		return this.id;
	}
}
