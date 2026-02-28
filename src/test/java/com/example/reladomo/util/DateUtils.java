package com.example.reladomo.util;

import java.sql.Timestamp;

public final class DateUtils
{
    private DateUtils()
    {
    }

    public static Timestamp ts(String yyyyMmDd)
    {
        return Timestamp.valueOf(yyyyMmDd + " 00:00:00.000");
    }
}
