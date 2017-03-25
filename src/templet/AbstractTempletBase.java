package templet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import util.CommonDB;
import util.LogMgr;

/**
 * 抽象模板基类,抽取了一些公共方法
 * 
 * @author YangJian 2010-04-30
 * @since 3.0
 */
public abstract class AbstractTempletBase implements TempletBase {

	public int tmpID = 0;// 模板类型

	public String tmpName = null;// 模板名称

	public String edition = null;// 版本

	public String tmpFileName = null;// 模板文件

	public int tmpType;

	protected static Logger log = LogMgr.getInstance().getSystemLogger();

	public AbstractTempletBase() {
		super();
	}

	@Override
	public void buildTmp(int tmpID) {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = CommonDB.getConnection();
			if (conn == null) {
				log.error("模板解析失败(模板编号=" + tmpID + "),原因:无法获取数据库连接.");
				return;
			}

			String sql = "select * from IGP_CONF_TEMPLET t where TMPID =" + tmpID;
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();

			if (rs.next()) {
				this.tmpID = tmpID;
				this.tmpName = rs.getString("TMPNAME");
				this.edition = rs.getString("EDITION");
				this.tmpFileName = rs.getString("TEMPFILENAME");
				this.tmpType = rs.getInt("TMPTYPE");
				parseTemp(this.tmpFileName);
			}
		} catch (Exception e) {
			log.error("模板解析失败(模板编号=" + tmpID + "),原因:", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}
	}

	@Override
	public void buildTmp(TempletRecord record) {
		if (record != null) {
			this.tmpID = record.getId();
			this.edition = record.getEdition();
			this.tmpName = record.getName();
			this.tmpFileName = record.getFileName();
			try {
				parseTemp(this.tmpFileName);
			} catch (Exception e) {
				log.error("模板解析失败(模板编号=" + tmpID + "),原因:", e);
			}
		}
	}

	@Override
	public abstract void parseTemp(String tempContent) throws Exception;

	// 获取当前节点的值
	protected String getNodeValue(Node CurrentNode) {
		String strValue = "";
		NodeList nodelist = CurrentNode.getChildNodes();
		if (nodelist != null) {
			for (int i = 0; i < nodelist.getLength(); i++) {
				Node tempnode = nodelist.item(i);
				if (tempnode.getNodeType() == Node.TEXT_NODE) {
					strValue = tempnode.getNodeValue();
				}
			}
		}
		return strValue;
	}

	// SubFields子节点下面是否存在元素
	protected boolean existSubField(Node CurrentNode) {
		boolean IsExist = false;
		NodeList nodelist = CurrentNode.getChildNodes();
		if (nodelist != null) {
			for (int i = 0; i < nodelist.getLength(); i++) {
				Node tempnode = nodelist.item(i);
				if (tempnode.getNodeType() == Node.ELEMENT_NODE) {
					NodeList sublist = tempnode.getChildNodes();
					if (sublist != null) {
						IsExist = true;
					}
				}
			}
		}
		return IsExist;
	}

	/* 利用java Dom 读取配置的时候,当读到遇到换行符号的时候,\n存储到字符串中确是\\n 影响解析,在此进行换行符号的转换 */
	protected String WrapPromptChange(String Keyword) {
		char ch = (char) 10;
		int nIndex = Keyword.indexOf("\\n");
		while (nIndex >= 0) {
			String Head = Keyword.substring(0, nIndex);

			String Tail = "";
			if (nIndex + 2 < Keyword.length())
				Tail = Keyword.substring(nIndex + 2, Keyword.length());

			Keyword = Head + String.valueOf(ch) + Tail;
			nIndex = Keyword.indexOf("\\n");
		}

		Keyword = Keyword.replaceAll("\\|", "\\\\|");
		return Keyword;
	}
}
