package com.seedoilz.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    public  String getId( T input);
    public  String getTableName();
    public  void join(T input, JSONObject dim);
}
