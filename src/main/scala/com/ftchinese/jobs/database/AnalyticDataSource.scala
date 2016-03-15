package com.ftchinese.jobs.database

import java.util.Properties

import org.springframework.jdbc.datasource.DriverManagerDataSource

/**
 * Analytic database source
 * Created by GWB on 2014/12/11.
 */
class AnalyticDataSource extends DriverManagerDataSource{
    def AnalyticDataSource(): Unit ={
        val prop = new Properties()
        prop.put("connectionProperties", "characterEncoding=utf-8")
        this.setDriverClassName("org.springframework.jdbc.datasource.DriverManagerDataSource")
        this.setConnectionProperties(prop)
    }
}
