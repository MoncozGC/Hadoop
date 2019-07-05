package com.air.antispider.stream.common.util.string;

import org.apache.commons.lang.StringUtils;

public class StringUtil {
    /**
     * 根据指定的字符串位置进行字符串的替换
     * @param sourceStr          原始字符串
     * @param startIndex        起始位置
     * @param endIndex          结束位置
     * @param newStr           要替换的内容
     * @return
     */
    public static String replace(String sourceStr, int startIndex, int endIndex, String newStr){
        StringBuilder sb = new StringBuilder(sourceStr);
        if(StringUtils.isNotBlank(sourceStr)){
            //将开始位置到结束位置之间的字符串替换成新的字符串
            sb.replace(startIndex, endIndex, newStr);
        }
        return sb.toString();
    }
}
