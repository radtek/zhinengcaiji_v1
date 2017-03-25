package parser.eric.pm;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;

import util.LogMgr;
import util.Util;

//2011-11-29
// CLT_PM_W_ERIC_HSDSCHRESOURCES表的PMREPORTEDCQI拆分成PMREPORTEDCQI_0、PMREPORTEDCQI_1、PMREPORTEDCQI_2、……PMREPORTEDCQI_31共32个单独的counter
public class CQIHandler {

	synchronized static void handCQI(File txt, File ctl) {
		InputStream txtIn = null;
		InputStream ctlIn = null;
		PrintWriter txtWriter = null;
		PrintWriter ctlWriter = null;
		try {
			txtIn = new FileInputStream(txt);
			ctlIn = new FileInputStream(ctl);
			StringWriter tmp = new StringWriter();
			IOUtils.copy(txtIn, tmp);
			tmp.flush();
			tmp.close();
			String txtContent = tmp.toString();
			tmp = new StringWriter();
			IOUtils.copy(ctlIn, tmp);
			tmp.flush();
			tmp.close();
			String ctlContent = tmp.toString();
			IOUtils.closeQuietly(txtIn);
			IOUtils.closeQuietly(ctlIn);
			txtWriter = new PrintWriter(txt);
			ctlWriter = new PrintWriter(ctl);
			String[] items = txtContent.split("\n");
			int cqiCounterIndex = -1;
			for (int i = 0; i < items.length; i++) {
				String line = items[i];
				if (i == 0) {
					String[] sp = line.split(";");
					for (int j = 0; j < sp.length; j++) {
						if (sp[j].equalsIgnoreCase("PMREPORTEDCQI")) {
							cqiCounterIndex = j;
							break;
						}
					}
					if (cqiCounterIndex < 0) {
						txtWriter.print(txtContent);
						ctlWriter.print(ctlContent);
						break;
					}
					txtWriter.print(line + ";");
					for (int cqiIndex = 0; cqiIndex < 32; cqiIndex++) {
						txtWriter.print("PMREPORTEDCQI_" + cqiIndex);
						if (cqiIndex < 31)
							txtWriter.print(";");
					}
					txtWriter.println();
				} else {
					txtWriter.print(line + ";");
					String[] sp = line.split(";");
					String cqiRaw = sp[cqiCounterIndex];
					StringBuilder buff = new StringBuilder();
					if (Util.isNull(cqiRaw)) {
						for (int cqiIndex = 0; cqiIndex < 32; cqiIndex++) {
							buff.append("");
							if (cqiIndex < 31)
								buff.append(";");
						}
					} else {
						String[] sp2 = cqiRaw.split(",");
						for (int cqiIndex = 0; cqiIndex < 32; cqiIndex++) {
							String val = "";
							if (cqiIndex < sp2.length)
								val = cqiIndex < 32 ? sp2[cqiIndex] : "";
							buff.append(val);
							if (cqiIndex < 31)
								buff.append(";");
						}
					}
					txtWriter.println(buff);
				}
			}

			if (cqiCounterIndex > -1) {
				StringBuilder ctlBuff = new StringBuilder(ctlContent);
				ctlBuff.delete(ctlBuff.lastIndexOf(")"), ctlBuff.length());
				ctlBuff.append(",");
				for (int cqiIndex = 0; cqiIndex < 32; cqiIndex++) {
					ctlBuff.append("PMREPORTEDCQI_" + cqiIndex);
					if (cqiIndex < 31)
						ctlBuff.append(",");
				}
				ctlBuff.append(")");
				ctlWriter.println(ctlBuff);
			}
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("处理CQI字段出错，txt=" + txt + "，ctl=" + ctl, e);
		} finally {
			IOUtils.closeQuietly(txtWriter);
			IOUtils.closeQuietly(ctlWriter);
		}
	}

	public static void main(String[] args) {
		// handCQI(new
		// File("E:\\datacollector_path\\ldrlog\\eric_w_pm\\755123\\755123_CLT_PM_W_ERIC_HSDSCHRESOURCES_20111125104500_152524151_10475659850355.txt"),
		// new
		// File("E:\\datacollector_path\\ldrlog\\eric_w_pm\\755123\\755123_CLT_PM_W_ERIC_HSDSCHRESOURCES_20111125104500_152524151_10475659850355.ctl"));
		String[] ar = new String[]{"", ""};
		System.out.println(ar[2]);
	}
}
