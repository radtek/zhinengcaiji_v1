package parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;

/**
 * 尝试解析话单文件。
 * 
 * @author ChenSijiang
 */
public class CHRParseTest {

	private static final int BUFFER_SIZE = 64; // 缓冲长度

	private static final int FILE_FLAG = 0xefefefef;// 文件是否完好的标志(EFEFEFEF)

	public static void main(String[] args) throws Exception {
		double curr = System.currentTimeMillis();
		File file = new File("C:\\Documents and Settings\\ChenSijiang\\桌面\\CHR_2_20100919153500.dat");
		InputStream in = new FileInputStream(file);

		byte[] buff = new byte[BUFFER_SIZE];
		in.read(buff, 0, 4);// 00000000h00~00000000h03
		System.out.println("文件是否完好的标志=" + (toInt(buff) == FILE_FLAG));
		in.read(buff, 0, 4);// 00000000h04~00000000h07
		System.out.println("测量单元MU的数量=" + toDword(buff));
		in.read(buff, 0, 4);// 00000000h08~00000000h0b
		int period = toDword(buff);

		// start time
		in.read(buff, 0, 7);// 00000000h0c~00000010h02
		MuFileDateTimeType stime = MuFileDateTimeType.asByteBuffer(buff);
		in.read(buff, 0, 1);// 00000010h03
		boolean sisDst = (buff[0] != 0);
		in.read(buff, 0, 2);// 00000010h04~00000010h05
		int stimezone = toShort(buff);
		in.read(buff, 0, 2);// 00000010h06~00000010h07
		int sdstOffset = toShort(buff);

		// end time
		in.read(buff, 0, 7);// 00000010h08~00000010h0e
		MuFileDateTimeType etime = MuFileDateTimeType.asByteBuffer(buff);
		in.read(buff, 0, 1);// 00000010h0f
		boolean eisDst = (buff[0] != 0);
		in.read(buff, 0, 2);// 00000020h00~00000020h01
		int etimezone = toShort(buff);
		in.read(buff, 0, 2);// 00000020h02~00000020h03
		int edstOffset = toShort(buff);

		MuFileTimeInfo muFileTimeInfo = new MuFileTimeInfo(period, new MuDstTimeType(stime, sisDst, stimezone, sdstOffset), new MuDstTimeType(etime,
				eisDst, etimezone, edstOffset));
		System.out.println("文件的时间信息=" + muFileTimeInfo);

		// 测量单元内容
		in.read(buff, 0, 2);
		int muId = toShort(buff);
		in.read(buff, 0, 4);
		int recordNum = toDword(buff);
		in.read(buff, 0, 4);
		int muLength = toDword(buff);
		in.read(buff, 0, 2);
		int miNum = toShort(buff);
		MIInfo[] miInfos = new MIInfo[miNum];
		for (int i = 0; i < miNum; i++) {
			in.read(buff, 0, 2);
			int miId = toShort(buff);
			in.read(buff, 0, 1);
			int miType = buff[0];
			in.read(buff, 0, 4);
			int miLength = toDword(buff);
			miInfos[i] = new MIInfo(miId, miType, miLength);
		}
		MITypeVecInfo miTypeVecInfo = new MITypeVecInfo(miNum, miInfos);
		MuResultInfo muResultInfo = new MuResultInfo(muId, recordNum, muLength, miTypeVecInfo, null);
		System.out.println("MuResultInfo=" + muResultInfo);

		// CHR部分
		in.read(buff, 0, 18);
		System.out.println("HANDLE_INDEX=" + toShort(buff));
		in.read(buff, 0, 50);
		System.out.println(buff[0]);
		in.close();
		System.out.println("耗时=" + (System.currentTimeMillis() - curr) / 1000.00);
	}

	public static int toInt(byte[] buff) {
		if (buff != null && buff.length >= 4) {
			return ((buff[0] & 0xff) << 24) + ((buff[1] & 0xff) << 16) + ((buff[2] & 0xff) << 8) + ((buff[3] & 0xff) << 0);
		} else {
			return -1;
		}
	}

	public static int toDword(byte[] buff) {
		if (buff != null && buff.length >= 4) {
			return ((buff[3] & 0xff) << 24) + ((buff[2] & 0xff) << 16) + ((buff[1] & 0xff) << 8) + ((buff[0] & 0xff) << 0);
		} else {
			return -1;
		}
	}

	public static int toShort(byte[] buff) {
		if (buff != null && buff.length >= 2) {
			return ((buff[1] & 0xff) << 8) + ((buff[0] & 0xff) << 0);
		} else {
			return -1;
		}
	}

	public static int toShort1(byte[] buff) {
		if (buff != null && buff.length >= 2) {
			return ((buff[0] & 0xff) << 8) + ((buff[1] & 0xff) << 0);
		} else {
			return -1;
		}
	}

	public static boolean findBit(byte b, int bitIndex) {
		return false;
	}
}

class MuFileDateTimeType {

	int year;

	int month;

	int day;

	int hour;

	int minute;

	int second;

	private MuFileDateTimeType(int year, int month, int day, int hour, int minute, int second) {
		super();
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.minute = minute;
		this.second = second;
	}

	public static MuFileDateTimeType asByteBuffer(byte[] buff) throws Exception {
		if (buff != null && buff.length >= 7) {
			int year = ((buff[1] & 0xff) << 8) + ((buff[0] & 0xff) << 0);
			int month = (buff[2] & 0xff);
			int day = (buff[3] & 0xff);
			int hour = (buff[4] & 0xff);
			int minute = (buff[5] & 0xff);
			int second = (buff[6] & 0xff);
			return new MuFileDateTimeType(year, month, day, hour, minute, second);
		}
		return null;
	}

	@Override
	public String toString() {

		return year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;
	}

}

class MuDstTimeType {

	MuFileDateTimeType muFileDateTimeType;

	boolean isDst;

	int timezone;

	int dstOffset;

	public MuDstTimeType(MuFileDateTimeType muFileDateTimeType, boolean isDst, int timezone, int dstOffset) {
		super();
		this.muFileDateTimeType = muFileDateTimeType;
		this.isDst = isDst;
		this.timezone = timezone;
		this.dstOffset = dstOffset;
	}

	@Override
	public String toString() {
		return "MuDstTimeType [日期时间=" + muFileDateTimeType + ", 是否执行夏令时=" + isDst + ", 时区=" + timezone + ", 夏令时偏移=" + dstOffset + "]";
	}

}

class MuFileTimeInfo {

	int period;

	MuDstTimeType startTime;

	MuDstTimeType endTime;

	public MuFileTimeInfo(int period, MuDstTimeType startTime, MuDstTimeType endTime) {
		super();
		this.period = period;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	@Override
	public String toString() {
		return "MuFileTimeInfo [统计周期(秒)=" + period + ", 开始时间=" + startTime + ", 结束时间=" + endTime + "]";
	}

}

class MuResultInfo {

	int muId;

	int recordNum;

	int muLength;

	MITypeVecInfo miTypeVecInfo;

	byte[] moRst;

	public MuResultInfo(int muId, int recordNum, int muLength, MITypeVecInfo miTypeVecInfo, byte[] moRst) {
		super();
		this.muId = muId;
		this.recordNum = recordNum;
		this.muLength = muLength;
		this.miTypeVecInfo = miTypeVecInfo;
		this.moRst = moRst;
	}

	@Override
	public String toString() {
		return "MuResultInfo [测量单元标识=" + muId + ", 本MU记录数=" + recordNum + ", 本测量单元所占的字节=" + muLength + ", 本MU包含MI信息=" + miTypeVecInfo + ", CHR记录数据="
				+ Arrays.toString(moRst) + "]";
	}

}

class MITypeVecInfo {

	int miNum;

	MIInfo[] miList;

	public MITypeVecInfo(int miNum, MIInfo[] miList) {
		super();
		this.miNum = miNum;
		this.miList = miList;
	}

	@Override
	public String toString() {
		return "MITypeVecInfo [MI数量=" + miNum + ", MI说明=" + Arrays.toString(miList) + "]";
	}

}

class MIInfo {

	int miId;

	int miType;

	int miLength;

	public MIInfo(int miId, int miType, int miLength) {
		super();
		this.miId = miId;
		this.miType = miType;
		this.miLength = miLength;
	}

	@Override
	public String toString() {
		return "MIInfo [MI标识=" + miId + ", MI数据类型=" + miType + ", MI数据长度=" + miLength + "]";
	}

}
