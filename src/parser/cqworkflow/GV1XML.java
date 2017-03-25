package parser.cqworkflow;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import collect.FTPConfig;
import framework.SystemConfig;

/**
 * G网重庆工单配置，XML文件部分。
 * 
 * @author ChenSijiang 2011-2-24 下午06:45:19
 */
public class GV1XML extends Parser {

	private String logKey;

	private String stamptime;

	private String omcid;

	private Timestamp tsStamptime;

	private Timestamp tsCollecttime;

	private File currDir;

	private int insertCount = 0;

	private String tableName;

	private static final String SQL_CONFIG = "insert into clt_taskinfo_btsconfig (omcid, collecttime, stamptime, "
			+ "workflowid, formname, formno, fillman, sendtime, remark, formstatus, recordman, "
			+ "recordtime, nettype, siteid, sitename, address, configreason, oldconfigvalue,"
			+ " configvalue, finalconfigvalue, filename, acceptman, ownerleader, makeman)values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String SQL_MODIFY = "insert into clt_taskinfo_modifybts (omcid, collecttime, stamptime,"
			+ " workflowid, formname, formno, fillman, sendtime, remark, formstatus, recordman, "
			+ "acceptman, recordtime, nettype, ownerleader, siteid, sitename, sectorid, azimuthold,"
			+ " azimuthad, azimuthfinal, poold, poad, pofinal, eoold, eoad, eofinal, antennatypeold,"
			+ "antennatypead, antennatypefinal, isshareold, issharead, issharefinal, filename) values"
			+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	// 甘肃用
	private static final String SQL_MODIFY_GS = "insert into clt_taskinfo_modifybts_gs (omcid, collecttime, stamptime,"
			+ " workflowid, formname, formno, fillman, sendtime, remark, formstatus, recordman, "
			+ "acceptman, recordtime, nettype, ownerleader, siteid, sitename, sectorid, azimuthold,"
			+ " azimuthad, azimuthfinal, poold, poad, pofinal, eoold, eoad, eofinal, antennatypeold,"
			+ "antennatypead, antennatypefinal, isshareold, issharead, issharefinal, filename, lac, ci, cell_name) values"
			+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	boolean isGS;

	private static final String SQL_WIREMODIF = "insert into clt_taskinfo_wireargumodify "
			+ "(omcid,  collecttime,  stamptime,  workflowid,  formname,  formno,  fillman,  "
			+ "sendtime,  remark,  formstatus,  nettype,  manufacturername,  auditman,  exeman, "
			+ " begintime,  endtime,  applytime,  docycle,  netelementtype,  ifinfluenceservice,"
			+ "  netelementname,  argumentname,  argumentbefor,  argumentafter,  argumentlast, "
			+ " filename )values (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, " + " ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?)";

	private static final String SQL_3G_ARGMODIF = "insert into clt_taskinfo_3g_argmodif\n" + "(\n" + "omcid         ,\n" + "collecttime   ,\n"
			+ "stamptime     ,\n" + "xml_formno    ,\n" + "group_no      ,\n" + "apply_time    ,\n" + "apply_com     ,\n" + "apply_person  ,\n"
			+ "start_time    ,\n" + "end_time      ,\n" + "modif_level      ,\n" + "rnc_id        ,\n" + "lac        ,\n" + "ci            ,\n"
			+ "modif_ne      ,\n" + "arg_name      ,\n" + "arg_before    ,\n" + "arg_final     ,\n" + "modif_reason  ,\n" + "form_id       ,\n"
			+ "remarks\n" + ")\n" + "values\n" + "(\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n"
			+ "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?\n" + ")";

	private static final String SQL_SOFTUPGRADE = "insert into clt_taskinfo_cqsoftupgrade\n" + "  (omcid,\n" + "   collecttime,\n"
			+ "   stamptime,\n" + "   workflowid,\n" + "   formname,\n" + "   formno,\n" + "   fillman,\n" + "   sendtime,\n" + "   remark,\n"
			+ "   formstatus,\n" + "   recordman,\n" + "   recordtime,\n" + "   acceptman,\n" + "   csdept,\n" + "   audit1man,\n"
			+ "   impactbuss,\n" + "   starttime,\n" + "   endtime,\n" + "   swtype,\n" + "   factory,\n" + "   nename,\n" + "   edtionbefore,\n"
			+ "   edtionlater,\n" + "   disman,\n" + "   netype,\n" + "   devicetype,\n" + "   filename0,\n" + "   filename1)\n" + "values\n"
			+ "  (?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n"
			+ "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n"
			+ "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?,\n" + "   ?)";

	private static final String SQL_3G_ADJ = "insert into clt_taskinfo_3g_adj\n" + "(\n" + "omcid               ,\n" + "collecttime         ,\n"
			+ "stamptime           ,\n" + "xml_formno          ,\n" + "group_no            ,\n" + "apply_time          ,\n"
			+ "apply_com           ,\n" + "apply_person        ,\n" + "start_time          ,\n" + "end_time            ,\n"
			+ "modif_level            ,\n" + "rnc_id              ,\n" + "lac              ,\n" + "ci                  ,\n"
			+ "cell_name           ,\n" + "adj_cell_type       ,\n" + "adj_rnc_id          ,\n" + "adj_ci              ,\n"
			+ "adj_cell_name       ,\n" + "modif_del           ,\n" + "modif_reason        ,\n" + "form_id             ,\n" + "remarks\n"
			+ ")values\n" + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String SQL_3G_23GEXTADJ = "insert into clt_taskinfo_3g_23gextadj\n" + "(\n" + "omcid           ,\n" + "collecttime     ,\n"
			+ "stamptime       ,\n" + "xml_formno      ,\n" + "group_no        ,\n" + "apply_time      ,\n" + "apply_com       ,\n"
			+ "apply_person    ,\n" + "start_time      ,\n" + "end_time        ,\n" + "modif_level        ,\n" + "rnc_id_3g       ,\n"
			+ "ext_adj_type    ,\n" + "adj_lac         ,\n" + "adj_ci          ,\n" + "adj_cell_name   ,\n" + "modif_del       ,\n"
			+ "arg_name        ,\n" + "arg_before      ,\n" + "arg_final       ,\n" + "modif_reason    ,\n" + "form_id         ,\n" + "remarks\n"
			+ ")\n" + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String SQL_3G_ADJPARA = "insert into clt_taskinfo_3g_adjpara\n" + "(\n" + "omcid          ,\n" + "collecttime    ,\n"
			+ "stamptime      ,\n" + "xml_formno     ,\n" + "group_no       ,\n" + "apply_time     ,\n" + "apply_com      ,\n" + "apply_person   ,\n"
			+ "start_time     ,\n" + "end_time       ,\n" + "modif_level       ,\n" + "rnc_id         ,\n" + "lac            ,\n"
			+ "ci             ,\n" + "cell_name      ,\n" + "ajd_net_type   ,\n" + "adj_lac        ,\n" + "adj_ci         ,\n" + "adj_cell_name  ,\n"
			+ "arg_name       ,\n" + "arg_before     ,\n" + "arg_final      ,\n" + "modif_reason   ,\n" + "form_id        ,\n" + "remarks\n"
			+ ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String SQL_DEFICIENCY = "insert into clt_taskinfo_deficiency(OMCID,COLLECTTIME,STAMPTIME,FILENAME,FORMNO,NETTYPE,REMARK)"
			+ " values (?,?,?,?,?,?,?)";

	@Override
	public synchronized boolean parseData() throws Exception {
		if (fileName.endsWith(".xls") || fileName.endsWith(".xlsx"))
			return true;
		insertCount = 0;
		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		this.tsCollecttime = new Timestamp(System.currentTimeMillis());
		this.tsStamptime = new Timestamp(collectObjInfo.getLastCollectTime().getTime());
		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), this.stamptime);

		try {
			File xmlFile = new File(fileName);
			isGS = xmlFile != null && xmlFile.getName().toLowerCase().contains("gsmodify");
			currDir = xmlFile.getParentFile();
			if (xmlFile.getName().toLowerCase().contains("btsconfig"))
				parseConfig(xmlFile);
			else if (xmlFile.getName().toLowerCase().contains("modifybts"))
				parseModify(xmlFile);
			else if (xmlFile.getName().toLowerCase().contains("wirelessargumodify"))
				parseWireModif(xmlFile);
			else if (xmlFile.getName().toLowerCase().contains("softupgrad"))
				parseSoftupgrade(xmlFile);

			return true;
		} catch (Exception e) {
			log.error(logKey + "解析文件出错", e);
			return false;
		} finally {
			CommonDB.closeDBConnection(con, ps, null);
			con = null;
			ps = null;
			LogMgr.getInstance().getDBLogger()
					.log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, insertCount, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + insertCount + "  表名=" + tableName);
			tableName = null;
		}

	}

	@SuppressWarnings("unchecked")
	private void parseConfig(File xmlFile) throws Exception {
		tableName = "CLT_TASKINFO_BTSCONFIG";
		log.debug(logKey + "开始解析 - " + xmlFile.getAbsolutePath());
		Document doc = new SAXReader().read(xmlFile);
		Element root = doc.getRootElement();

		List<Element> workflowInfoElements = root.elements("workflowInfo");

		if (workflowInfoElements == null || workflowInfoElements.size() == 0)
			log.warn(logKey + "未找到workflowInfo节点，文件 - " + xmlFile.getAbsolutePath());

		for (Element workflowInfoElement : workflowInfoElements) {
			storeConfig(workflowInfoElement);
		}
		log.debug(logKey + "解析完成 - " + xmlFile.getAbsolutePath());
	}

	@SuppressWarnings("unchecked")
	private void parseWireModif(File xmlFile) throws Exception {
		tableName = "CLT_TASKINFO_WIREARGUMODIFY";
		log.debug(logKey + "开始解析 - " + xmlFile.getAbsolutePath());
		Document doc = new SAXReader().read(xmlFile);
		Element root = doc.getRootElement();

		List<Element> workflowInfoElements = root.elements("workflowInfo");

		if (workflowInfoElements == null || workflowInfoElements.size() == 0)
			log.warn(logKey + "未找到workflowInfo节点，文件 - " + xmlFile.getAbsolutePath());

		for (Element workflowInfoElement : workflowInfoElements) {
			storeWireModif(workflowInfoElement);
		}
		log.debug(logKey + "解析完成 - " + xmlFile.getAbsolutePath());
	}

	@SuppressWarnings("unchecked")
	private void parseSoftupgrade(File xmlFile) throws Exception {
		tableName = "CLT_TASKINFO_CQSOFTUPGRADE";
		log.debug(logKey + "开始解析 - " + xmlFile.getAbsolutePath());
		Document doc = new SAXReader().read(xmlFile);
		Element root = doc.getRootElement();

		List<Element> workflowInfoElements = root.elements("workflowInfo");

		if (workflowInfoElements == null || workflowInfoElements.size() == 0)
			log.warn(logKey + "未找到workflowInfo节点，文件 - " + xmlFile.getAbsolutePath());

		for (Element workflowInfoElement : workflowInfoElements) {
			storeSoftupgrade(workflowInfoElement);
		}
		log.debug(logKey + "解析完成 - " + xmlFile.getAbsolutePath());
	}

	@SuppressWarnings("unchecked")
	private void parseModify(File xmlFile) throws Exception {

		if (!isGS)
			tableName = "CLT_TASKINFO_MODIFYBTS";
		else
			tableName = "CLT_TASKINFO_MODIFYBTS_GS";

		log.debug(logKey + "开始解析 - " + xmlFile.getAbsolutePath());
		Document doc = new SAXReader().read(xmlFile);
		Element root = doc.getRootElement();

		List<Element> workflowInfoElements = root.elements("workflowInfo");

		if (workflowInfoElements == null || workflowInfoElements.size() == 0)
			log.warn(logKey + "未找到workflowInfo节点，文件 - " + xmlFile.getAbsolutePath());

		for (Element workflowInfoElement : workflowInfoElements) {
			storeModify(workflowInfoElement);
		}
		log.debug(logKey + "解析完成 - " + xmlFile.getAbsolutePath());
	}

	Connection con = null;

	PreparedStatement ps = null;

	private synchronized void storeSoftupgrade(Element workflowInfoElement) {
		if (con == null)
			con = DbPool.getConn();

		String workFlowID = workflowInfoElement.attributeValue("workFlowID");
		String formName = find2(workflowInfoElement, "FormName");
		String formNo = find2(workflowInfoElement, "FormNo");
		String fillMan = find2(workflowInfoElement, "FillMan");
		String sendTime = find2(workflowInfoElement, "SendTime");
		String remark = find2(workflowInfoElement, "Remark");
		String formStatus = find2(workflowInfoElement, "FormStatus");
		String recordMan = find2(workflowInfoElement, "RecordMan");
		String recordTime = find2(workflowInfoElement, "RecordTime");
		String acceptMan = find2(workflowInfoElement, "AcceptMan");
		String csDept = find2(workflowInfoElement, "CsDept");
		String audit1Man = find2(workflowInfoElement, "Audit1Man");
		String impactBuss = find2(workflowInfoElement, "ImpactBuss");
		String startTime = find2(workflowInfoElement, "StartTime");
		String endTime = find2(workflowInfoElement, "EndTime");
		String swType = find2(workflowInfoElement, "SwType");
		String factory = find2(workflowInfoElement, "Factory");
		String neName = find2(workflowInfoElement, "NeName");
		String editionBefore = find2(workflowInfoElement, "EdtionBefore");
		String edtionLater = find2(workflowInfoElement, "EdtionLater");
		String disMan = find2(workflowInfoElement, "DisMan");
		String neType = find2(workflowInfoElement, "NeType");
		String deviceType = find2(workflowInfoElement, "DeviceType");
		String fileName0 = find2(workflowInfoElement, "FileName0");
		String fileName1 = find2(workflowInfoElement, "FileName1");

		try {
			ps = con.prepareStatement(SQL_SOFTUPGRADE);
			ps.setInt(1, Integer.parseInt(omcid));
			ps.setTimestamp(2, tsCollecttime);
			ps.setTimestamp(3, tsStamptime);
			ps.setInt(4, Integer.parseInt(workFlowID));
			ps.setString(5, formName);
			ps.setString(6, formNo);
			ps.setString(7, fillMan);
			ps.setTimestamp(8, parseTime(sendTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setString(9, remark);
			ps.setString(10, formStatus);
			ps.setString(11, recordMan);
			ps.setTimestamp(12, parseTime(recordTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setString(13, acceptMan);
			ps.setString(14, csDept);
			ps.setString(15, audit1Man);
			ps.setString(16, impactBuss);
			ps.setTimestamp(17, parseTime(startTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setTimestamp(18, parseTime(endTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setString(19, swType);
			ps.setString(20, factory);
			ps.setString(21, neName);
			ps.setString(22, editionBefore);
			ps.setString(23, edtionLater);
			ps.setString(24, disMan);
			ps.setString(25, neType);
			ps.setString(26, deviceType);
			ps.setString(27, fileName0);
			ps.setString(28, fileName1);

			if (ps.executeUpdate() > 0)
				insertCount++;
			CommonDB.close(null, ps, null);
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常", e);
		}

	}

	private synchronized void storeModify(Element workflowInfoElement) {
		try {
			if (con == null)
				con = DbPool.getConn();

			String workFlowID = workflowInfoElement.attributeValue("workFlowID");
			String formName = find2(workflowInfoElement, "FormName");
			String formNo = find2(workflowInfoElement, "FormNo");
			String fillMan = find2(workflowInfoElement, "FillMan");
			String sendTime = find2(workflowInfoElement, "SendTime");
			String remark = find2(workflowInfoElement, "Remark");
			String formStatus = find2(workflowInfoElement, "FormStatus");
			String recordMan = find2(workflowInfoElement, "RecordMan");
			String acceptMan = find2(workflowInfoElement, "AcceptMan");
			String recordTime = find2(workflowInfoElement, "RecordTime");
			String fileName = find2(workflowInfoElement, "FileName");
			String lac = find2(workflowInfoElement, "LAC");
			String ci = find2(workflowInfoElement, "CI");
			String cellname = find2(workflowInfoElement, "Name");

			List<String> nts = find1(workflowInfoElement, "NetType");
			List<String> ols = find1(workflowInfoElement, "OwnerLeader");
			List<String> sis = find1(workflowInfoElement, "SiteID");
			List<String> sns = find1(workflowInfoElement, "SiteName");
			List<String> seis = find1(workflowInfoElement, "SectorId");
			List<String> aos = find1(workflowInfoElement, "AzimuthOld");
			List<String> aas = find1(workflowInfoElement, "AzimuthAD");
			List<String> afs = find1(workflowInfoElement, "AzimuthFinal");
			List<String> pos = find1(workflowInfoElement, "PoOld");
			List<String> pas = find1(workflowInfoElement, "PoAd");
			List<String> pfs = find1(workflowInfoElement, "PoFinal");
			List<String> eos = find1(workflowInfoElement, "EoOld");
			List<String> eas = find1(workflowInfoElement, "EoAd");
			List<String> efs = find1(workflowInfoElement, "EoFinal");
			List<String> atos = find1(workflowInfoElement, "AntennaTypeOld");
			List<String> atas = find1(workflowInfoElement, "AntennaTypeAd");
			List<String> atfs = find1(workflowInfoElement, "AntennaTypeFinal");
			List<String> isos = find1(workflowInfoElement, "IsShareOld");
			List<String> isas = find1(workflowInfoElement, "IsShareAd");
			List<String> isfs = find1(workflowInfoElement, "IsShareFinal");

			int size = nts.size();
			for (int i = 0; i < size; i++) {
				if (!isGS)
					ps = con.prepareStatement(SQL_MODIFY);
				else
					ps = con.prepareStatement(SQL_MODIFY_GS);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, workFlowID);
				ps.setString(5, formName);
				ps.setString(6, formNo);
				ps.setString(7, fillMan);
				ps.setTimestamp(8, parseTime(sendTime, "yyyy-MM-dd HH:mm:ss"));
				ps.setString(9, remark);
				ps.setString(10, formStatus);
				ps.setString(11, recordMan);
				ps.setString(12, acceptMan);
				ps.setTimestamp(13, parseTime(recordTime, "yyyy-MM-dd HH:mm:ss"));

				ps.setString(14, nts.size() > i ? nts.get(i) : "");
				ps.setString(15, ols.size() > i ? ols.get(i) : "");
				ps.setString(16, sis.size() > i ? sis.get(i) : "");
				ps.setString(17, sns.size() > i ? sns.get(i) : "");
				ps.setString(18, seis.size() > i ? seis.get(i) : "");
				ps.setString(19, aos.size() > i ? aos.get(i) : "");
				ps.setString(20, aas.size() > i ? aas.get(i) : "");
				ps.setString(21, afs.size() > i ? afs.get(i) : "");
				ps.setString(22, pos.size() > i ? pos.get(i) : "");
				ps.setString(23, pas.size() > i ? pas.get(i) : "");
				ps.setString(24, pfs.size() > i ? pfs.get(i) : "");
				ps.setString(25, eos.size() > i ? eos.get(i) : "");
				ps.setString(26, eas.size() > i ? eas.get(i) : "");
				ps.setString(27, efs.size() > i ? efs.get(i) : "");
				ps.setString(28, atos.size() > i ? atos.get(i) : "");
				ps.setString(29, atas.size() > i ? atas.get(i) : "");
				ps.setString(30, atfs.size() > i ? atfs.get(i) : "");
				ps.setString(31, isos.size() > i ? isos.get(i) : "");
				ps.setString(32, isas.size() > i ? isas.get(i) : "");
				ps.setString(33, isfs.size() > i ? isfs.get(i) : "");
				ps.setString(34, fileName);

				if (isGS) {
					ps.setString(35, lac);
					ps.setString(36, ci);
					ps.setString(37, cellname);
				}

				if (ps.executeUpdate() > 0)
					insertCount++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常", e);
		}
	}

	private synchronized void storeConfig(Element workflowInfoElement) {
		try {
			if (con == null)
				con = DbPool.getConn();

			String workFlowID = workflowInfoElement.attributeValue("workFlowID");
			String formName = find2(workflowInfoElement, "FormName");
			String formNo = find2(workflowInfoElement, "FormNo");
			String fillMan = find2(workflowInfoElement, "FillMan");
			String sendTime = find2(workflowInfoElement, "SendTime");
			String remark = find2(workflowInfoElement, "Remark");
			String formStatus = find2(workflowInfoElement, "FormStatus");
			String recordMan = find2(workflowInfoElement, "RecordMan");
			String acceptMan = find2(workflowInfoElement, "AcceptMan");
			String recordTime = find2(workflowInfoElement, "RecordTime");
			String ownerLeader = find2(workflowInfoElement, "OwnerLeader");
			String makeMan = find2(workflowInfoElement, "MakeMan");
			String fileName = find2(workflowInfoElement, "FileName");

			List<String> nts = find1(workflowInfoElement, "NetType");
			List<String> sis = find1(workflowInfoElement, "SiteID");
			List<String> sns = find1(workflowInfoElement, "SiteName");
			List<String> ads = find1(workflowInfoElement, "Address");
			List<String> crs = find1(workflowInfoElement, "ConfigReason");
			List<String> ocvs = find1(workflowInfoElement, "OldConfigValue");
			List<String> cvs = find1(workflowInfoElement, "ConfigValue");
			List<String> fcvs = find1(workflowInfoElement, "FinalConfigValue");

			int size = nts.size();
			for (int i = 0; i < size; i++) {
				ps = con.prepareStatement(SQL_CONFIG);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, workFlowID);
				ps.setString(5, formName);
				ps.setString(6, formNo);
				ps.setString(7, fillMan);
				ps.setTimestamp(8, parseTime(sendTime, "yyyy-MM-dd HH:mm:ss"));
				ps.setString(9, remark);
				ps.setString(10, formStatus);
				ps.setString(11, recordMan);
				ps.setTimestamp(12, parseTime(recordTime, "yyyy-MM-dd HH:mm:ss"));
				ps.setString(13, nts.size() > i ? nts.get(i) : "");
				ps.setString(14, sis.size() > i ? sis.get(i) : "");
				ps.setString(15, sns.size() > i ? sns.get(i) : "");
				ps.setString(16, ads.size() > i ? ads.get(i) : "");
				ps.setString(17, crs.size() > i ? crs.get(i) : "");
				ps.setString(18, ocvs.size() > i ? ocvs.get(i) : "");
				ps.setString(19, cvs.size() > i ? cvs.get(i) : "");
				ps.setString(20, fcvs.size() > i ? fcvs.get(i) : "");
				ps.setString(21, fileName);
				ps.setString(22, acceptMan);
				ps.setString(23, ownerLeader);
				ps.setString(24, makeMan);
				if (ps.executeUpdate() > 0)
					insertCount++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常", e);
		}
	}

	private synchronized void storeWireModif(Element workflowInfoElement) {
		String formNo = null;
		String netType = null;
		String fileName = null;

		try {
			if (con == null)
				con = DbPool.getConn();

			String workFlowID = workflowInfoElement.attributeValue("workFlowID");
			String formName = find2(workflowInfoElement, "FormName");
			formNo = find2(workflowInfoElement, "FormNo");
			String fillMan = find2(workflowInfoElement, "FillMan");
			String sendTime = find2(workflowInfoElement, "SendTime");
			String remark = find2(workflowInfoElement, "Remark");
			String formStatus = find2(workflowInfoElement, "FormStatus");
			netType = find2(workflowInfoElement, "NetType");
			String manufacturerName = find2(workflowInfoElement, "ManufacturerName");
			String auditMan = find2(workflowInfoElement, "AuditMan");
			String exeMan = find2(workflowInfoElement, "ExeMan");
			String beginTime = find2(workflowInfoElement, "BeginTime");
			String endTime = find2(workflowInfoElement, "EndTime");
			String applyTime = find2(workflowInfoElement, "ApplyTime");
			String doCycle = find2(workflowInfoElement, "DoCycle");
			String netElementType = find2(workflowInfoElement, "NETELEMENTTYPE");
			String ifInfluenceService = find2(workflowInfoElement, "IfInfluenceService");
			String netElementName = find2(workflowInfoElement, "NetElementName");
			String argumentName = find2(workflowInfoElement, "ArgumentName");
			String argumentBefor = find2(workflowInfoElement, "ArgumentBefor");
			String argumentAfter = find2(workflowInfoElement, "ArgumentAfter");
			String argumentLast = find2(workflowInfoElement, "ArgumentLast");
			fileName = find2(workflowInfoElement, "FileName");

			ps = con.prepareStatement(SQL_WIREMODIF);
			ps.setString(1, this.omcid);
			ps.setTimestamp(2, this.tsCollecttime);
			ps.setTimestamp(3, this.tsStamptime);
			ps.setString(4, workFlowID);
			ps.setString(5, formName);
			ps.setString(6, formNo);
			ps.setString(7, fillMan);
			ps.setTimestamp(8, parseTime(sendTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setString(9, remark);
			ps.setString(10, formStatus);
			ps.setString(11, netType);
			ps.setString(12, manufacturerName);
			ps.setString(13, auditMan);
			ps.setString(14, exeMan);
			ps.setTimestamp(15, parseTime(beginTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setTimestamp(16, parseTime(endTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setTimestamp(17, parseTime(applyTime, "yyyy-MM-dd HH:mm:ss"));
			ps.setString(18, doCycle);
			ps.setString(19, netElementType);
			ps.setString(20, ifInfluenceService);
			ps.setString(21, netElementName);
			ps.setString(22, argumentName);
			ps.setString(23, argumentBefor);
			ps.setString(24, argumentAfter);
			ps.setString(25, argumentLast);
			ps.setString(26, fileName);
			if (ps.executeUpdate() > 0)
				insertCount++;
			CommonDB.close(null, ps, null);

			File xlsFile = new File(currDir, fileName);
			if (!xlsFile.exists()) {
				// 如果是重庆---liangww add 2012-12-07 增加工单文件缺失入到缺失表
				if (!isGS) {
					// 电子运维没有正常传递G/W(即NETTYPE)网工单'网络公司[20121210]无线参数调整012 '(即FORMNO)，系统无法正常采集
					// 电子运维没有正常传递%s网工单'%s'，系统无法正常采集
					String deficiencyRemark = String.format("电子运维没有正常传递%s网工单'%s'，系统无法正常采集", netType, formNo);
					storeDeficiency(fileName, formNo, netType, deficiencyRemark);
				}

				//
				log.warn(logKey + "未找到xls文件 - " + xlsFile.getAbsolutePath() + " FormNo - " + formNo);
			} else {
				parseWireModifXls(xlsFile, formNo);
				FTPConfig cfg = FTPConfig.getFTPConfig(collectObjInfo.getTaskID());
				boolean isDel = SystemConfig.getInstance().isDeleteWhenOff();
				if (cfg != null)
					isDel = cfg.isDelete();
				if (isDel)
					xlsFile.delete();
			}
		} catch (ParseException e) {
			// 如果是重庆---liangww add 2012-12-18 增加工单文件解析错误时入到缺失表
			if (!isGS) {
				// /W(即NETTYPE)网工单'网络公司[20121210]无线参数调整012 '(即FORMNO)与工单模板不一致，系统无法正常采集！！
				// /%s网工单'%s'与工单模板不一致，系统无法正常采集！！
				String deficiencyRemark = String.format("%s网工单'%s'与工单模板不一致，系统无法正常采集！！", netType, formNo);
				storeDeficiency(fileName, formNo, netType, deficiencyRemark);
			}
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常", e);
		}
	}

	/**
	 * 存储缺失表
	 * 
	 * @param fileName
	 * @param formNo
	 * @param netType
	 * @param remark
	 */
	public void storeDeficiency(String fileName, String formNo, String netType, String remark) {
		try {
			ps = con.prepareStatement(SQL_DEFICIENCY);
			ps.setString(1, this.omcid);
			ps.setTimestamp(2, this.tsCollecttime);
			ps.setTimestamp(3, this.tsStamptime);

			// OMCID,COLLECTTIME,STAMPTIME,FILENAME,FORMNO,NETTYPE,REMARK)
			ps.setString(4, fileName);
			ps.setString(5, formNo);
			ps.setString(6, netType);
			ps.setString(7, remark);
			ps.execute();
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常", e);
		} finally {
			CommonDB.close(null, ps, null);
		}
	}

	private static final String XLS_SQL_GSM_CM_MODIF_RECORD = "insert into clt_taskinfo_basicargument "
			+ "(omcid,collecttime, stamptime,initialformno,vgroup,sendtime ,fillunit ,fillman  "
			+ ",begintime,endtime  ,argumentlevel  ,bscname  ,btsname  ,lac,cellname ,ci ,trxnumber,"
			+ "argumentname,parameterold,parameterfinal ,reason,formno,remark) values (? ,? , ? ,? "
			+ ",? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? )";

	private static final String XLS_SQL_2G_ADJ_CM = "insert into clt_taskinfo_adargument (omcid ,"
			+ "collecttime ,stamptime ,initialformno ,vgroup,sendtime,fillunit,fillman ,begintime "
			+ ",endtime ,argumentlevel ,bscname ,btsname ,cellname,ci,lac ,adbsc ,adcellname,adci,"
			+ "adlac ,argumentname,parameterold,parameterfinal,reason,formno,remark) values (?,?,?"
			+ ",?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )";

	private static final String XLS_SQL_2G_ADJ_RELAT = "insert into clt_taskinfo_adrelation (omcid,"
			+ "collecttime,stamptime,initialformno,vgroup ,sendtime ,fillunit ,fillman,begintime,"
			+ "endtime,argumentlevel,bscname,btsname,cellname ,ci ,lac,adbsc,adcellname ,adci ,"
			+ "adlac,operation,reason ,formno ,remark ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String XLS_SQL_23G_ADJ = "insert into clt_taskinfo_23gadrelation "
			+ "(omcid , collecttime ,stamptime ,initialformno ,vgroup,sendtime,fillunit,"
			+ "fillman ,begintime ,endtime ,argumentlevel ,bscname ,btsname ,cellname,"
			+ "ci,lac ,rncname ,threegcellid,threegcellname,threegcellcode,threegfrequency ,"
			+ "threeglac ,operation ,formno,remark) values (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static Sheet[] findAllSheets(Workbook xls) throws Exception {
		int size = xls.getNumberOfSheets();
		List<Sheet> list = new ArrayList<Sheet>();
		for (int i = 0; i < size; i++) {
			list.add(xls.getSheetAt(i));
		}
		return list.toArray(new Sheet[0]);
	}

	private void parseWireModifXls(File xlsFile, String formNo) throws ParseException {
		FileInputStream fis = null;
		try {
			log.debug(logKey + "开始解析xls文件 - " + xlsFile.getAbsolutePath() + "  FormNo - " + formNo);
			fis = new FileInputStream(xlsFile);
			Workbook xls = null;
			try {
				xls = new HSSFWorkbook(fis);
			} catch (Exception e) {
				IOUtils.closeQuietly(fis);
				fis = new FileInputStream(xlsFile);
				xls = new XSSFWorkbook(fis);
			}
			Sheet[] sheets = findAllSheets(xls);
			for (Sheet sheet : sheets) {
				String sheetName = sheet.getSheetName().trim();

				if (sheetName.equals("GSM参数修改记录")) {
					String tableName = "CLT_TASKINFO_BASICARGUMENT";
					store_XLS_SQL_GSM_CM_MODIF_RECORD(sheet, formNo, tableName);
				} else if (sheetName.equals("2G邻区参数(含3G)")) {
					String tableName = "CLT_TASKINFO_ADARGUMENT";
					store_XLS_SQL_2G_ADJ_CM(sheet, formNo, tableName);
				} else if (sheetName.equals("2G邻区关系")) {
					String tableName = "CLT_TASKINFO_ADRELATION";
					store_XLS_SQL_2G_ADJ_RELAT(sheet, formNo, tableName);
				} else if (sheetName.equals("23G互操作邻区")) {
					String tableName = "CLT_TASKINFO_23GADRELATION";
					store_XLS_SQL_23G_ADJ(sheet, formNo, tableName);
				} else if (sheetName.equals("WCDMA参数修改表")) {
					String tableName = "clt_taskinfo_3g_argmodif";
					store_XLS_SQL_3G_ARGMODIF(sheet, formNo, tableName);
				} else if (sheetName.equals("小区邻区增减表")) {
					String tableName = "clt_taskinfo_3g_adj";
					store_XLS_SQL_3G_ADJ(sheet, formNo, tableName);
				} else if (sheetName.equals("23G外部邻区定义")) {
					String tableName = "clt_taskinfo_3g_23gextadj";
					store_XLS_SQL_3G_23GEXTADJ(sheet, formNo, tableName);
				} else if (sheetName.equals("邻区参数修改表")) {
					String tableName = "clt_taskinfo_3g_adjpara";
					store_XLS_SQL_3G_ADJPARA(sheet, formNo, tableName);
				}
			}
		} catch (Exception e) {
			log.error(logKey + "xls文件解析失败 - " + xlsFile.getAbsolutePath() + "  FormNo - " + formNo, e);

			throw new ParseException(e);
		} finally {
			IOUtils.closeQuietly(fis);
		}
		log.debug(logKey + "xls文件解析完毕 - " + xlsFile.getAbsolutePath() + "  FormNo - " + formNo);
	}

	private Cell[] findRowCells(Row row) {
		if (row == null)
			return null;
		List<Cell> list = new ArrayList<Cell>();
		for (int i = 0; i < row.getPhysicalNumberOfCells(); i++) {
			list.add(row.getCell(i));
		}
		if (list.size() == 0)
			return null;
		return list.toArray(new Cell[0]);
	}

	private void store_XLS_SQL_GSM_CM_MODIF_RECORD(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();

			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(XLS_SQL_GSM_CM_MODIF_RECORD);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				ps.setTimestamp(6, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ps.setTimestamp(9, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setTimestamp(10, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	private void store_XLS_SQL_2G_ADJ_CM(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(XLS_SQL_2G_ADJ_CM);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				ps.setTimestamp(6, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ps.setTimestamp(9, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setTimestamp(10, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				ps.setString(24, getCellVal(cells, cellIndex++));
				ps.setString(25, getCellVal(cells, cellIndex++));
				ps.setString(26, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	Cell[] heads = null;

	private void store_XLS_SQL_2G_ADJ_RELAT(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(XLS_SQL_2G_ADJ_RELAT);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				ps.setTimestamp(6, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ps.setTimestamp(9, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setTimestamp(10, parseTime(getCellVal(cells, cellIndex++), "yy-MM-dd"));
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				ps.setString(24, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	private boolean isDateCell(Cell cell, String head) {
		try {
			return (head != null && (head.toLowerCase().contains("时间") || head.toLowerCase().contains("日期") || head.toLowerCase().contains("date") || head
					.toLowerCase().contains("time")));
		} catch (Exception e) {
		}
		return false;
	}

	private String getCellVal(Cell[] cells, int cellIndex) {
		if (cellIndex >= cells.length || cells[cellIndex] == null)
			return "";

		String headName = "";
		if (heads != null) {
			headName = heads[cellIndex].getStringCellValue();
		}

		if (isDateCell(cells[cellIndex], headName))
			return Util.getDateString(cells[cellIndex].getDateCellValue());
		else if (cells[cellIndex].getCellType() == Cell.CELL_TYPE_STRING) {
			return cells[cellIndex].getStringCellValue();
		} else if (cells[cellIndex].getCellType() == Cell.CELL_TYPE_NUMERIC) {
			String v = String.valueOf(cells[cellIndex].getNumericCellValue());
			if (v != null && v.trim().length() > 2 && v.trim().endsWith(".0"))
				v = v.trim().substring(0, v.trim().length() - 2);
			return v;
		} else if (cells[cellIndex].getCellType() == Cell.CELL_TYPE_BLANK)
			return "";
		else if (cells[cellIndex].getCellType() == Cell.CELL_TYPE_BOOLEAN)
			return String.valueOf(cells[cellIndex].getBooleanCellValue());

		return "";
	}

	private void store_XLS_SQL_3G_ADJPARA(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(SQL_3G_ADJPARA);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				Timestamp ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(6, ts);
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(9, ts);
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(10, ts);
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				ps.setString(24, getCellVal(cells, cellIndex++));
				ps.setString(25, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	private void store_XLS_SQL_3G_23GEXTADJ(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(SQL_3G_23GEXTADJ);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				Timestamp ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(6, ts);
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(9, ts);
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(10, ts);
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	private void store_XLS_SQL_3G_ADJ(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(SQL_3G_ADJ);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				Timestamp ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(6, ts);
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(9, ts);
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(10, ts);
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	private void store_XLS_SQL_3G_ARGMODIF(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(SQL_3G_ARGMODIF);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				Timestamp ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(6, ts);
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(9, ts);
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(10, ts);
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	private void store_XLS_SQL_23G_ADJ(Sheet sheet, String formNo, String tableName) {
		Connection con = null;
		PreparedStatement ps = null;
		int count = 0;
		try {
			con = DbPool.getConn();
			int rowCount = sheet.getPhysicalNumberOfRows();
			heads = findRowCells(sheet.getRow(0));
			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = null;
				try {
					cells = findRowCells(sheet.getRow(i));
				} catch (ArrayIndexOutOfBoundsException unused) {
					continue;
				}
				if (cells == null || cells[0] == null || Util.isNull(cells[0].getStringCellValue()))
					continue;
				int cellIndex = 0;
				ps = con.prepareStatement(XLS_SQL_23G_ADJ);
				ps.setString(1, this.omcid);
				ps.setTimestamp(2, this.tsCollecttime);
				ps.setTimestamp(3, this.tsStamptime);
				ps.setString(4, formNo);
				ps.setString(5, getCellVal(cells, cellIndex++));
				Timestamp ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(6, ts);
				ps.setString(7, getCellVal(cells, cellIndex++));
				ps.setString(8, getCellVal(cells, cellIndex++));
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(9, ts);
				ts = null;
				try {
					ts = new Timestamp(Util.getDate(getCellVal(cells, cellIndex++), "yy-MM-dd").getTime());
				} catch (Exception e) {
				}
				ps.setTimestamp(10, ts);
				ps.setString(11, getCellVal(cells, cellIndex++));
				ps.setString(12, getCellVal(cells, cellIndex++));
				ps.setString(13, getCellVal(cells, cellIndex++));
				ps.setString(14, getCellVal(cells, cellIndex++));
				ps.setString(15, getCellVal(cells, cellIndex++));
				ps.setString(16, getCellVal(cells, cellIndex++));
				ps.setString(17, getCellVal(cells, cellIndex++));
				ps.setString(18, getCellVal(cells, cellIndex++));
				ps.setString(19, getCellVal(cells, cellIndex++));
				ps.setString(20, getCellVal(cells, cellIndex++));
				ps.setString(21, getCellVal(cells, cellIndex++));
				ps.setString(22, getCellVal(cells, cellIndex++));
				ps.setString(23, getCellVal(cells, cellIndex++));
				ps.setString(24, getCellVal(cells, cellIndex++));
				ps.setString(25, getCellVal(cells, cellIndex++));
				if (ps.executeUpdate() > 0)
					count++;
				CommonDB.close(null, ps, null);
			}
		} catch (Exception e) {
			log.error(logKey + "xls入库异常", e);
		} finally {
			CommonDB.close(null, ps, con);
			LogMgr.getInstance().getDBLogger().log(collectObjInfo.getDevInfo().getOmcID(), tableName, tsStamptime, count, collectObjInfo.getTaskID());
			log.debug(logKey + "入库条数=" + count + "  表名=" + tableName);
		}
	}

	@SuppressWarnings("unchecked")
	private List<String> find1(Element workflowInfoElement, String enName) {
		List<String> result = new ArrayList<String>();
		List<Element> es = workflowInfoElement.elements("fieldInfo");
		for (Element e : es) {
			String name = e.elementText("fieldEnName");
			if (name != null && name.equalsIgnoreCase(enName))
				result.add(e.elementText("fieldValue"));
		}

		return result;
	}

	private String find2(Element workflowInfoElement, String enName) {
		String s = "";
		List<String> list = find1(workflowInfoElement, enName);
		if (list.size() > 0) {
			s = list.get(0);
			if (s.contains(".doc") && enName.equalsIgnoreCase("filename")) {
				try {
					s = list.get(1);
				} catch (Exception e) {
				}
			}
		}
		return s;
	}

	static Timestamp parseTime(String val, String fmt) {
		try {
			Timestamp ts = new Timestamp(Util.getDate(val, fmt).getTime());
			return ts;
		} catch (Exception e) {
			return null;
		}
	}

	static private final class ParseException extends Exception {

		public ParseException() {
			super();
		}

		public ParseException(Throwable casue) {
			super(casue);
		}
	}

	public static void main(String[] args) {
		// String a = null;
		// String b = null;
		// boolean c = false;
		// if(a == null ||(c = (b == null)) == true){
		// System.out.println(c);
		// }

		String sss = "2021.0";

		if (sss.endsWith(".0")) {
			System.out.println(sss.trim().substring(0, sss.trim().length() - 2));
		}

		CollectObjInfo obj = new CollectObjInfo(22501);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(2250101);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		GV1XML w = new GV1XML();
		w.fileName = "C:\\Users\\ChenSijiang\\Desktop\\CQSoftUpgrade.xml";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
