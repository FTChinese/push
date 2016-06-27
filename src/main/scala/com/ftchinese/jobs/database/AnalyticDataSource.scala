package com.ftchinese.jobs.database

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
 * Analytic database source
 * Created by GWB on 2014/12/11.
 */
class AnalyticDataSource {
    private val ds = new ComboPooledDataSource()

    ds.setMinPoolSize(1)
    ds.setMaxPoolSize(100)
    ds.setAcquireIncrement(1)

    def setUrl(url: String): Unit ={
        ds.setJdbcUrl(url)
    }

    def setUsername(username: String): Unit ={
        ds.setUser(username)
    }

    def setPassword(password: String): Unit ={
        ds.setPassword(password)
    }

    def getConnection: Connection = ds.getConnection
}
