package com.ftchinese.jobs.database

import java.sql.Connection

import com.zaxxer.hikari.HikariDataSource

/**
 * Analytic database source
 * Created by GWB on 2014/12/11.
 */
class AnalyticDataSource {
    private val ds = new HikariDataSource()

    ds.setMaximumPoolSize(10)

    def setUrl(url: String): Unit ={
        ds.setJdbcUrl(url)
    }

    def setUsername(username: String): Unit ={
        ds.setUsername(username)
    }

    def setPassword(password: String): Unit ={
        ds.setPassword(password)
    }

    def getConnection: Connection = ds.getConnection
}
