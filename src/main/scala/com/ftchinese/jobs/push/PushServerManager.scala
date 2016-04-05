package com.ftchinese.jobs.push

import java.net.Socket
import javax.net.ssl.{KeyManagerFactory, SSLContext}

/**
 * Push server manager.
 * Created by wanbo on 16/3/21.
 */
class PushServerManager(keyName: String, keyPassword: String) {
    private var server_host = ""
    private var server_port: Int = _

    def getPushServer(production: Boolean): Socket ={

        if(production){
            server_host = PushServer.pro_host
            server_port = PushServer.pro_port
        } else {
            server_host = PushServer.dev_host
            server_port = PushServer.dev_port
        }

        val context = SSLContext.getInstance("TLS")

        val kmf = KeyManagerFactory.getInstance("sunx509")

        kmf.init(KeystoreManager.loadKeystore(keyName), KeystoreManager.getKeystorePassword)

        context.init(kmf.getKeyManagers, null, null)

        val factory = context.getSocketFactory

        val socket = factory.createSocket(server_host, server_port)
        socket.setSoTimeout(3000)

        socket
    }
}
