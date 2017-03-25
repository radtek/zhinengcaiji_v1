package parser.eric.pm;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import util.CommonDB;
import util.Util;

public class TruncateTable {

	public static void main(String[] args) {
		String tnames = "clt_cm_g_eric_v1_raepp, clt_cm_g_eric_v1_rlbcp, clt_cm_g_eric_v1_rlcfp, clt_cm_g_eric_v1_rlchp, clt_cm_g_eric_v1_rlcpp, clt_cm_g_eric_v1_rlcrp, clt_cm_g_eric_v1_rlcxp, clt_cm_g_eric_v1_rldep, clt_cm_g_eric_v1_rldep_ext, clt_cm_g_eric_v1_rlgsp, clt_cm_g_eric_v1_rlhpp, clt_cm_g_eric_v1_rlihp, clt_cm_g_eric_v1_rlimp, clt_cm_g_eric_v1_rllbp, clt_cm_g_eric_v1_rllcp, clt_cm_g_eric_v1_rlldp, clt_cm_g_eric_v1_rllfp, clt_cm_g_eric_v1_rllhp, clt_cm_g_eric_v1_rllop, clt_cm_g_eric_v1_rllpp, clt_cm_g_eric_v1_rlmfp, clt_cm_g_eric_v1_rlnrp, clt_cm_g_eric_v1_rlpcp, clt_cm_g_eric_v1_rlsbp, clt_cm_g_eric_v1_rlssp, clt_cm_g_eric_v1_rxmop";
		String[] items = tnames.split(",");
		List<String> names = new ArrayList<String>();

		for (String s : items) {
			if (Util.isNotNull(s)) {
				names.add(s.trim());
			}
		}

		String sql = "truncate table ";

		for (String s : names) {
			try {
				CommonDB.executeUpdate(sql + s);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}
