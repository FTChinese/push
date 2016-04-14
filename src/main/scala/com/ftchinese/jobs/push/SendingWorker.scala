package com.ftchinese.jobs.push

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat

import akka.actor.Actor
import com.ftchinese.jobs.common.{Logging, NotificationMessage}

/**
 * Sending worker
 * Created by wanbo on 16/3/25.
 */
class SendingWorker() extends Actor with Logging {

    override def receive: Receive = {
        case notificationMessage: NotificationMessage =>

            if(validateNotification(notificationMessage)){

                //log.info(notificationMessage.toJson)

                send(notificationMessage)
            } else {
                log.warn("This is not a valid notification !! " + notificationMessage.toJson)
            }

        case _ => // Ignore
    }

    /**
     * Verify the notification is valid
     *
     * If the notification creating date is not current date will be deny.
     *
     * @param note  The notification will be sent.
     * @return
     */
    def validateNotification(note: NotificationMessage): Boolean ={
        var isValid = true

        val sdf = new SimpleDateFormat("")

        if(sdf.format(System.currentTimeMillis()) != sdf.format(note.createTime)) {
            isValid = false
        }

        isValid
    }

    def send(note: NotificationMessage): Unit ={

        try {

            log.info("------------ Start to send [%d] ------------".format(note.id))
            log.info("Sending details:" + note.toJson)

            val pushServer = new PushServerManager(note.deviceType + note.appNumber, "")

            val socket = pushServer.getPushServer(note.production)

            val payload = new Payload()

            payload.setAlert(note.text)
            payload.setBadge(note.badge)
            if(note.sound != "")
                payload.setSound(note.sound)

            payload.addCustomDictionary("action", note.action)
            payload.addCustomDictionary("id", note.label)
            payload.addCustomDictionary("lead", note.lead)

            log.info("Sending payload:" + payload.getPayload)

            val out = socket.getOutputStream

            out.write(payloadStream(id = note.id, token = note.deviceToken, payload = payload.getPayload).toByteArray)

            out.flush()

            val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
            val ooo = new ByteArrayOutputStream()

            var k = in.read()
            while (k != -1) {
                ooo.write(k)
                k = in.read()
            }

            val oooArr = ooo.toByteArray

            oooArr.foreach(println)

            log.info("id:" + ((oooArr(2) << 24) + (oooArr(3) << 16) + (oooArr(4) << 8) + oooArr(5)))

            out.close()
            in.close()

            socket.close()
        } catch {
            case e: Exception =>
                log.error("Error when send:" + e)
        }
    }

    def payloadStream(id: Int, token: String, payload: String): ByteArrayOutputStream = {

        val tokenBytes = token.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray

        val bao = new ByteArrayOutputStream()

        val command: Byte = 1

        bao.write(command)

        bao.write(intTo4ByteArray(id))

        bao.write(intTo4ByteArray(3))

        bao.write(intTo2ByteArray(tokenBytes.length))

        bao.write(tokenBytes)

        bao.write(intTo2ByteArray(payload.getBytes("UTF-8").length))

        bao.write(payload.getBytes("UTF-8"))

        bao.flush()

        bao
    }

    def intTo2ByteArray(value: Int): Array[Byte] ={
        val s1 = (value & 0xFF00) >> 8
        val s2 = value & 0xFF
        Array(s1.toByte, s2.toByte)
    }

    def intTo4ByteArray(value: Int): Array[Byte] ={
        ByteBuffer.allocate(4).putInt(value).array()
    }


}
