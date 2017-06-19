package com.bqss.analyze;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by darrenfantasy on 2017/6/14.
 */
public class DateToTimeStamp{
    public static Long evaluate(String date){
        SimpleDateFormat fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.CHINA);
        try {
            return fm.parse(date).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return 0L;
        }
    }
}
