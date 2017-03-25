package parser.dzlgzxt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import util.CommonDB;
import util.DbPool;

class MainInsert {

	static final String INSERT_SQL = "\n" + "insert into clt_cm_ty_dzlgzxt_softmgr\n" + " (\n" + " omcid             ,\n" + " collecttime       ,\n"
			+ " stamptime         ,\n" + " form_id           ,\n" + " send_time         ,\n" + " update_start_time ,\n" + " update_end_time   ,\n"
			+ " net_type          ,\n" + " update_type       ,\n" + " update_element    ,\n" + " device_type       ,\n" + " vendor_name       ,\n"
			+ " pre_version       ,\n" + " patch_version     ,\n" + " after_version\n" + " )\n" + " values\n" + " (\n" + " ?,\n" + " ?,\n" + " ?,\n"
			+ " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?,\n" + " ?\n" + " )";

	void insert(SoftMgrFormContent form, Timestamp stamptime, Timestamp collecttime, int omcId) throws Exception {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(INSERT_SQL);
			int index = 1;
			ps.setInt(index++, omcId);
			ps.setTimestamp(index++, collecttime);
			ps.setTimestamp(index++, stamptime);
			ps.setString(index++, form.formId);
			ps.setTimestamp(index++, form.sendDate);
			ps.setTimestamp(index++, form.updateStartTime);
			ps.setTimestamp(index++, form.updateEndTime);
			ps.setString(index++, form.netType);
			ps.setString(index++, form.updateType);
			ps.setString(index++, form.updateElement);
			ps.setString(index++, form.deviceType);
			ps.setString(index++, form.deviceVendor);
			ps.setString(index++, form.preVersion);
			ps.setString(index++, form.patchVersion);
			ps.setString(index++, form.afterVersion);
			ps.executeUpdate();
		} finally {
			CommonDB.close(rs, ps, con);
		}
	}
}
