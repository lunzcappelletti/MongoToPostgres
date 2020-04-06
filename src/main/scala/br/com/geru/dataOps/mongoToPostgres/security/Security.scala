package br.com.geru.dataOps.mongoToPostgres.security

import java.security.MessageDigest
import java.util

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import javax.xml.bind.DatatypeConverter

class Security {


  private val ALGO = "AES"
  //generate 128bit key
  private val keyStr = ""

  private def generateKey = {
    var keyValue = keyStr.getBytes("UTF-8")
    val sha = MessageDigest.getInstance("SHA-1")
    keyValue = sha.digest(keyValue)
    keyValue = util.Arrays.copyOf(keyValue, 16) // use only first 128 bit

    val key = new SecretKeySpec(keyValue, ALGO)
    key
  }

  def encrypt(Data: String): String = {
    val key = generateKey
    val c = Cipher.getInstance(ALGO)
    c.init(Cipher.ENCRYPT_MODE, key)
    val encVal = c.doFinal(Data.getBytes)
    val encryptedValue = DatatypeConverter.printBase64Binary(encVal)
    encryptedValue
  }

  def decrypt(encryptedData: String): String = {
    val key = generateKey
    val c = Cipher.getInstance(ALGO)
    c.init(Cipher.DECRYPT_MODE, key)
    val decordedValue = DatatypeConverter.parseBase64Binary(encryptedData)
    val decValue = c.doFinal(decordedValue)
    val decryptedValue = new String(decValue)
    decryptedValue
  }
}
