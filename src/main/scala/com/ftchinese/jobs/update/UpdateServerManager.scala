package com.ftchinese.jobs.update

import java.net.Socket
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import com.ftchinese.jobs.common.KeystoreManager

/**
 * Update server manager.
 * Created by wanbo on 4/5/16.
 */
class UpdateServerManager(keyName: String, keyPassword: String) {
    private var server_host = ""
    private var server_port: Int = _

    def getPushServer(production: Boolean): Socket ={

        if(production){
            server_host = UpdateServer.pro_host
            server_port = UpdateServer.pro_port
        } else {
            server_host = UpdateServer.dev_host
            server_port = UpdateServer.dev_port
        }

        val context = SSLContext.getInstance("TLS")

        val kmf = KeyManagerFactory.getInstance("sunx509")

        kmf.init(KeystoreManager.loadKeystore(keyName), KeystoreManager.getKeystorePassword)

        context.init(kmf.getKeyManagers, null, null)

        val factory = context.getSocketFactory

        val socket = factory.createSocket(server_host, server_port)
        socket.setSoTimeout(30000)

        socket
    }
}
