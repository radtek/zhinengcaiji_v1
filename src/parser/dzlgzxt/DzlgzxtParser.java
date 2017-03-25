package parser.dzlgzxt;

import java.io.File;
import java.sql.Timestamp;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import util.LogMgr;
import util.Util;

/**
 * <p>
 * 电子流工作系统（dzlgzxt）软件版本管理工单采集。山东使用，采集天元生成的工单。
 * </p>
 * <p>
 * 有两类文件，一个是主CSV文件，里面记录工单的各个字段，比如“山东联通移维网调【2012】版本0066号[部省].csv”文件。
 * </p>
 * <p>
 * 还一类是反馈附件，不解析，是将它写到BLOB字段 。
 * </p>
 * <p>
 * 比如“山东联通移维网调【2012】版本0066号[部省].csv.青岛.1.附件b.xls”，里面“山东联通移维网调 【2012】版本0066号[部省]” 是工单号，“青岛”是反馈地市，“1”是附件类型，“附件b.xls”是真正的附件名。
 * </p>
 * <p>
 * 也就是说，前面“山东联通移维网调【2012 】版本0066号[部省].csv.青岛.1.”这一串东西，只是为了描述反馈附件“附件b.xls”是属于哪个工单、哪个地市、哪种类型的。
 * </p>
 * 
 * @author ChenSijiang 2012-5-25
 */
public class DzlgzxtParser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	int omcId;

	Timestamp stamptime;

	Timestamp collecttime;

	String logKey;

	/* 工单号。 */
	String formId;

	/* 文件名，不包含目录部分。 */
	String fullNameNoDir;

	/* 附件类型（编号）。 */
	int annexType;

	/* 附件文件名。 */
	String annexName;

	/* 反馈地市名。 */
	String feedbackArea;

	/* 当前文件是否是主CSV文件，比如“山东联通移维网调【2012】版本0066号[部省].csv”。 */
	boolean isMainCsv;

	MainCsvParser mainParser;

	MainInsert mainInsert;

	AnnexBlobInsert blobInsert;

	@Override
	public boolean parseData() throws Exception {
		try {
			initMembs();
		} catch (Exception e) {
			log.error(logKey + "初始化失败。", e);
			if (mainParser != null)
				mainParser.close();
			return false;
		}

		try {
			if (isMainCsv) {
				SoftMgrFormContent form = null;
				while (mainParser.hasNext()) {
					form = mainParser.next();
					if (form != null) {
						try {
							mainInsert.insert(form, stamptime, collecttime, omcId);
							log.debug(logKey + "工单入库成功：" + form.formId);
						} catch (Exception e) {
							log.error(logKey + "入库一条工单记录时异常：" + form, e);
						}
					}
				}
			} else {
				File file = new File(fileName);
				try {
					blobInsert.insert(formId, file.getAbsolutePath(), feedbackArea, annexName, annexType, file.length());
					log.debug(logKey + "成功入库了一个附件，工单号=" + formId + "，附件名=" + annexName + "，反馈地市=" + feedbackArea + "，文件大小=" + file.length());
				} catch (Exception e) {
					log.error(logKey + "入库附件异常，工单号=" + formId + "，附件名=" + annexName + "，反馈地市=" + feedbackArea + "，文件大小=" + file.length(), e);
				}
			}
		} catch (Exception e) {
			log.error(logKey + "处理工单文件时异常" + fileName, e);
			return false;
		} finally {
			if (mainParser != null)
				mainParser.close();
		}

		return true;
	}

	void initMembs() throws Exception {
		omcId = getCollectObjInfo().getDevInfo().getOmcID();
		stamptime = getCollectObjInfo().getLastCollectTime();
		collecttime = new Timestamp(System.currentTimeMillis());
		logKey = "[" + getCollectObjInfo().getTaskID() + "][" + Util.getDateString(stamptime) + "]";
		fullNameNoDir = FilenameUtils.getName(fileName);
		String[] sp = fullNameNoDir.split("\\.");
		if (sp.length == 2) {
			// 主工单内容，即“山东联通移维网调【2012】版本0066号[部省].csv”这样的文件，
			// 以“.”进行分隔，被分成两部分，以此判断。
			annexType = -1;
			annexName = null;
			feedbackArea = null;
			isMainCsv = true;
			mainInsert = new MainInsert();
		} else if (sp.length >= 6) {
			isMainCsv = false;
			blobInsert = new AnnexBlobInsert();
			// 是反馈单附件，即“山东联通移维网调【2012】版本0066号[部省].csv.东营.1.附件a.xls”这样的，
			// 以“.”进行分隔，被分成六部分。
			formId = sp[0];
			feedbackArea = sp[2];
			annexType = Integer.parseInt(sp[3]);

			/* 考虑到附件文件名中也可能有“.”号，所以把剩余部分都加在一起作为附件文件名。 */
			annexName = "";
			for (int i = 4; i < sp.length; i++) {
				annexName += sp[i];
				if (i < sp.length - 1)
					annexName += ".";
			}
		} else {
			throw new Exception("文件名非法：" + fullNameNoDir);
		}

		if (isMainCsv)
			mainParser = new MainCsvParser(fileName);
	}

	public static void main(String[] args) {
		System.out.println("山东联通移维网调【2012】版本0066号[部省].csv.东营.3.中国联通通信网实施反馈表(1).xls".substring(0,
				"山东联通移维网调【2012】版本0066号[部省].csv.东营.3.中国联通通信网实施反馈表(1).xls".indexOf(".")));
	}
}

// 涉及到的表：
/*-- Create table
 create table CLT_CM_TY_DZLGZXT_SOFTMGR
 (
 omcid             NUMBER,
 collecttime       DATE,
 stamptime         DATE,
 form_id           VARCHAR2(500),
 send_time         DATE,
 update_start_time DATE,
 update_end_time   DATE,
 net_type          VARCHAR2(100),
 update_type       VARCHAR2(100),
 update_element    VARCHAR2(500),
 device_type       VARCHAR2(100),
 vendor_name       VARCHAR2(100),
 pre_version       VARCHAR2(100),
 patch_version     VARCHAR2(100),
 after_version     VARCHAR2(100)
 );


 -- Add comments to the table 
 comment on table CLT_CM_TY_DZLGZXT_SOFTMGR
 is '电子流工作系统（dzlgzxt）软件版本管理工单采集';
 -- Add comments to the columns 
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.form_id
 is '工单编号';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.send_time
 is '派发时间';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.update_start_time
 is '升级开始时间';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.update_end_time
 is '升级结束时间';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.net_type
 is '网络类型';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.update_type
 is '升级类型';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.update_element
 is '升级的对象节点';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.device_type
 is '设备类型';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.vendor_name
 is '厂商名称';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.pre_version
 is '软/硬件申请前版本';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.patch_version
 is '新软件补丁号';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR.after_version
 is '升级后版本';*/
/*--
 * create table CLT_CM_TY_DZLGZXT_SOFTMGR_FILE
 (
 form_id       VARCHAR2(500),
 feedback_area VARCHAR2(100),
 annex_type    NUMBER,
 annex_len     NUMBER,
 annex_name    VARCHAR2(100),
 annex_content BLOB
 );

 -- Add comments to the table 
 comment on table CLT_CM_TY_DZLGZXT_SOFTMGR_FILE
 is '电子流工作系统（dzlgzxt）软件版本管理工单采集，附件表';
 -- Add comments to the columns 
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR_FILE.form_id
 is '工单编号，与CLT_CM_TY_DZLGZXT_SOFTMGR.form_id关联。';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR_FILE.feedback_area
 is '反馈者/反馈地市。';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR_FILE.annex_type
 is '1 = 升级执行情况附件列表；2 = 会议纪要；3 = 测试记录表；4 = 移动网管配置数据变更附件';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR_FILE.annex_len
 is '附件文件大小（字节）。';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR_FILE.annex_name
 is '附件文件名。';
 comment on column CLT_CM_TY_DZLGZXT_SOFTMGR_FILE.annex_content
 is '附件文件内容。';
 */
