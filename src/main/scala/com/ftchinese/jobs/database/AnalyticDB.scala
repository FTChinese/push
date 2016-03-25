package com.ftchinese.jobs.database

import java.sql.{Connection, PreparedStatement}

import com.ftchinese.jobs.common.Logging

/**
 * Analytic database class
 * Created by GWB on 2014/12/11.
 */
class AnalyticDB extends Logging {
    private var _dataSource: AnalyticDataSource = null
    private var _conn:Connection = null

    def setDataSource(ds: AnalyticDataSource){_dataSource = ds}

    /**
     * Execute sql query
     * @param sql sql string
     */
    def executeQuery(sql: String): Unit ={

        if(_conn == null || _conn.isClosed)
            connection()

        try {
            val ps: PreparedStatement = _conn.prepareStatement(sql)
            ps.executeUpdate()
            ps.close()
        } catch {
            case e: Exception =>
                throw e
        }
    }

    /**
     * Get area list
     * @return
     */
    def getMappingRules: List[(String, String, String)] ={
        var retList = List[(String, String, String)]()

        try{

            if(_conn == null || _conn.isClosed)
                connection()

            val sql = "select k.`key`, k.val, v.`name` from td_mappingkey k right join td_mappingval v on k.id = v.kid;"

            val ps = _conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                retList = retList :+ (rs.getString(1), rs.getString(2), rs.getString(3))
            }

        } catch {
            case e: Exception =>
                throw e
        }

        retList
    }

    /**
     * Get token list
     * @return
     */
    def getTokens(from: Long = 0, to: Long = 1): List[Map[String, String]] ={
        var dataList = List[Map[String, String]]()

        try{

            if(_conn == null || _conn.isClosed)
                connection()

            val sql = "SELECT * FROM analytic.ios_device_token where `timezone` = 'GMT 8' and device_type = 'phone' order by time_stamp desc limit %d, %d;".format(from, to)

            log.info(sql)

            val ps = _conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            val metaData = ps.getMetaData
            val columnCount = metaData.getColumnCount

            while (rs.next()){
                var tmpMap = Map[String, String]()

                for(i <- Range(1, columnCount + 1)) {
                    tmpMap = tmpMap + (metaData.getColumnLabel(i) -> rs.getString(i))
                }

                dataList = dataList :+ tmpMap
            }

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

    def getTestTokens(from: Long = 0, to: Long = 1): List[Map[String, String]] ={
        var dataList = List[Map[String, String]]()

        try{

            if(_conn == null || _conn.isClosed)
                connection()

            val sql = "select * from analytic.ios_device_token where device_token in ('b6c4eef757bcabff77b297211393d3c7801c24fed111b6198b2fceb029512d52') limit %d, %d;".format(from, to)

            log.info(sql)

            val ps = _conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            val metaData = ps.getMetaData
            val columnCount = metaData.getColumnCount

            while (rs.next()){
                var tmpMap = Map[String, String]()

                for(i <- Range(1, columnCount + 1)) {
                    tmpMap = tmpMap + (metaData.getColumnLabel(i) -> rs.getString(i))
                }

                dataList = dataList :+ tmpMap
            }

        } catch {
            case e: Exception =>
                throw e
        }

        dataList
    }

    /**
     * Get city list
     * @return
     */
    def getCities: List[(String, String)] ={
        var retList = List[(String, String)]()

        try{

            if(_conn == null || _conn.isClosed)
                connection()

            val sql = "SELECT pid,short_title FROM hrdata.tb_area where pid!=0 and short_title!='';"

            val ps = _conn.prepareStatement(sql)
            val rs = ps.executeQuery()

            while (rs.next()){
                retList = retList :+ (rs.getString(1), rs.getString(2))
            }

        } catch {
            case e: Exception =>
                throw e
        }

        retList
    }

    /**
     * Get database connection
     */
    protected def connection() = {
        try{
            if(_dataSource == null)
                throw new Exception("DataSource is empty!")

            _conn = _dataSource.getConnection

        } catch {
            case e: Exception =>
                log.error ("AnalyticDB reconnect error:", e)
        }
    }

    protected def close(): Unit ={
        try{
            if(_conn != null)
                _conn.close()
        } catch {
            case e: Exception =>
        }
    }
}
