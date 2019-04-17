package dc.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/**
 * 格式化时间
 */
public class TimeUtil {
	// 长度为17的时间日期格式
	private static final SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/**
	 * 取得当前时间
	 * 
	 * @return
	 */
	public static String getCurrentDateTime() {
		return sdf.format(Calendar.getInstance().getTime());
	}

	/**
	 * 取得当前日期
	 * 
	 * @return
	 */
	public static String getCurrentDate() {
		return sdf.format(Calendar.getInstance().getTime());
	}

	/**
	 * 格式化时间
	 * 
	 * @param time
	 * @return String
	 */
	public static String formatTime(long time) {
		return sdf.format(new Date(time));
	}

	/**
	 * 转换成毫秒时间
	 * 
	 * @param time
	 * @return long
	 */
	public static long parseLong(String time) {
		long returnTime = 0L;
		if (time != null && !"".equals(time)) {
			try {
				Date d = sdf.parse(time);
				returnTime = d.getTime();
			} catch (ParseException e) {
			}
		}

		return returnTime;
	}

	/**
	 * 计算时间差,将开始时间,比如"20000101 01:01:01",结束时间,比如"20000102 01:01:01"间的时间差计算出来,
	 * 并用毫秒表示
	 *
	 * @return long
	 */
	public static long calcTimeDiff(String sStartTime, String sEndTime) {
		return parseLong(sEndTime) - parseLong(sStartTime);
	}

	//i=1昨天，i=2前天
	public static Date getStartOfPastDay(int day){
		return getStartOfPastDay(System.currentTimeMillis(),day);
	}
	//i=1昨天，i=2前天
	public static Date getEndOfPastDay(int day){
		return getEndOfPastDay(System.currentTimeMillis(),day);
	}

	/**
	 * 获取某一时刻前n天的00:00:00.000，day=1某一时刻的前一天，day=0某一时刻的当天
	 * @param time
	 * @param day
	 * @return
	 */
	public static Date getStartOfPastDay(long time,int day){
		Calendar calendar=setCalendar(time,day,0,0,0,0);
		return calendar.getTime();
	}
	/**
	 * 获取某一时刻前n天的23:59:59.999，day=1某一时刻的前一天，day=0某一时刻的当天
	 * @param time
	 * @param day
	 * @return
	 */
	public static Date getEndOfPastDay(long time,int day){
		Calendar calendar=setCalendar(time,day,23,59,59,999);
		return calendar.getTime();
	}

	private static Calendar setCalendar(long time,int day,int hour,int minute,int second,int millisecond){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date(time));
		if(day>=0)calendar.set(Calendar.DATE,calendar.get(Calendar.DATE)-day);
		if(hour>=0)calendar.set(Calendar.HOUR_OF_DAY,hour);
		if(minute>=0)calendar.set(Calendar.MINUTE,minute);
		if(second>=0)calendar.set(Calendar.SECOND,second);
		if(millisecond>=0)calendar.set(Calendar.MILLISECOND,millisecond);
		return calendar;
	}

	/**
	 * 获取前n天的12:00:00.000，day=1昨天，day=0今天
	 * @param day
	 * @return
	 */
	public static Date get12OfPastDay(int day){
		Calendar calendar=setCalendar(System.currentTimeMillis(),day,12,0,0,0);
		return calendar.getTime();
	}
	//i=1昨天，i=2前天
	public static Date getTimeOfPastDay(long time,int day){
		Calendar calendar=setCalendar(time,day,-1,-1,-1,-1);
		return calendar.getTime();
	}

	public static long[] getTime(int day){
		long time[]=new long[2];
		time[0]=TimeUtil.getStartOfPastDay(day).getTime();
		time[1]=TimeUtil.getEndOfPastDay(day).getTime();
		return time;
	}
}
