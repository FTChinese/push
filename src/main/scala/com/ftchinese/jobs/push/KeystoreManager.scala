package com.ftchinese.jobs.push

import java.io.FileInputStream
import java.security.KeyStore

import com.ftchinese.jobs.common.Logging

/**
 * Keystore manager
 * Created by wanbo on 16/3/25.
 */
object KeystoreManager extends Logging {

    private val path = "../conf/"

    def loadKeystore(keyName: String): KeyStore ={

        val keyStore = KeyStore.getInstance("PKCS12")
        
        try {

            var fileName = ""

            keyName match {
                case "pad1" =>
                    fileName = ""
                case "phone2" =>
                    fileName = "mobile.p12"
                case _ =>
                    throw new Exception("Didn't find the keystore type !")
            }

            
            keyStore.load(new FileInputStream(path + fileName), getKeystorePassword)

        } catch {
            case e: Exception =>
                log.error("Load Keystore error:" + e)
        }

        keyStore
    }

    def getKeystorePassword: Array[Char] ={
        Array[Char]()
    }
}
