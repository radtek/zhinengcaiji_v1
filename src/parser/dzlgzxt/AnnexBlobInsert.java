package parser.dzlgzxt;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import oracle.sql.BLOB;

import org.apache.commons.io.IOUtils;

import util.CommonDB;
import util.DbPool;

class AnnexBlobInsert {

	static final String SQL_INSERT_EMPTY_BLOB = "insert into clt_cm_ty_dzlgzxt_softmgr_file "
			+ "(form_id,feedback_area,annex_type,annex_len,annex_name,annex_content) values (?,?,?,?,?,empty_blob())";

	static final String SQL_INSERT_FILE_TO_BLOB = "select annex_content from clt_cm_ty_dzlgzxt_softmgr_file "
			+ "where form_id=? and feedback_area=? and annex_type=? and annex_len=? and annex_name=? for update";

	void insert(String formId, String file, String feedbackArea, String annexName, int annexType, long len) throws Exception {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			con.setAutoCommit(false);
			ps = con.prepareStatement(SQL_INSERT_EMPTY_BLOB);
			int index = 1;
			ps.setString(index++, formId);
			ps.setString(index++, feedbackArea);
			ps.setInt(index++, annexType);
			ps.setLong(index++, len);
			ps.setString(index++, annexName);
			ps.executeUpdate();
			ps.close();
			index = 1;

			ps = con.prepareStatement(SQL_INSERT_FILE_TO_BLOB);
			ps.setString(index++, formId);
			ps.setString(index++, feedbackArea);
			ps.setInt(index++, annexType);
			ps.setLong(index++, len);
			ps.setString(index++, annexName);
			rs = ps.executeQuery();
			if (rs.next()) {
				Blob x = rs.getBlob(1);
				if (x != null) {
					BLOB blob = (BLOB) x;
					OutputStream out = null;
					InputStream in = null;
					try {
						out = blob.getBinaryOutputStream();
						in = new FileInputStream(file);
						IOUtils.copyLarge(in, out);
						out.flush();
					} finally {
						IOUtils.closeQuietly(out);
						IOUtils.closeQuietly(in);
					}
					con.commit();
				} else {
					throw new Exception("BLOB字段获取失败。");
				}
			}
		} catch (Exception e) {
			try {
				if (con != null)
					con.rollback();
			} catch (Exception ex) {
			}
			throw e;
		} finally {
			CommonDB.close(rs, ps, con);
		}
	}

	public static void main(String[] args) throws Exception {
		new AnnexBlobInsert().insert("asdf", "C:\\Users\\ChenSijiang\\Desktop\\HUB_PARA_ZTE_3.08_404_715_3_DO_20120524100000.zip", "岳阳",
				"HUB_PARA_ZTE_3.08_404_715_3_DO_20120524100000.zip", 1, 11111);
	}
}
