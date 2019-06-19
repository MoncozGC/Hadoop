package com.JadePenG.webLog.mapper;

import com.JadePenG.webLog.pojo.TAvgpvNum;

import java.util.List;

/**
 * @author Peng
 * @Description
 */
public interface TAvgpvNumMapper {
    List<TAvgpvNum> selectLastSeven(String s, String s1);
}
