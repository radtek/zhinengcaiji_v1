package db.pojo;

/**
 * 模板文件 -- 对象系统模板路径下的一个模板文件
 * 
 * @author YangJian
 * @since 1.0
 */
public class TempletFile {

	private String name; // 文件名称

	private long size; // 文件大小

	private String modifyDate; // 字符串格式的修改日期

	private String content; // 文件内容

	public TempletFile() {
		super();
	}

	public TempletFile(String name, long size, String modifyDate, String content) {
		super();
		this.name = name;
		this.size = size;
		this.modifyDate = modifyDate;
		this.content = content;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public String getModifyDate() {
		return modifyDate;
	}

	public void setModifyDate(String modifyDate) {
		this.modifyDate = modifyDate;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
