package com.twitter.finagle.exp.zookeeper.data

import com.twitter.io.Buf

case class ACL(perms: Int, id: Id) extends Data {
  def buf: Buf = Buf.Empty
    .concat(Buf.U32BE(perms))
    .concat(id.buf)
}

object ACL extends DataDecoder[ACL] {

  def apply(perm: Int, scheme: String, id: String): ACL =
    new ACL(perm, new Id(scheme, id))
  def apply(perm: Int, id: String) = parseACL(id + perm)(0)

  def unapply(buf: Buf): Option[(ACL, Buf)] = {
    val Buf.U32BE(perms, Id(id, rem)) = buf
    Some(ACL(perms, id), rem)
  }

  /**
   * Check an ACL list
   * @param aclList the ACL list to check
   */
  def check(aclList: Seq[ACL]): Unit = {
    aclList.map { acl =>
      acl.id.scheme match {
        case "world" => if (acl.id.data.toUpperCase != "ANYONE")
          throw new IllegalArgumentException(
            "ACL malformed exception (suggested: ((world:anyone), perms)")

        case "auth" => if (acl.id.data != "")
          throw new IllegalArgumentException(
            "ACL: Auth malformed exception (suggested: ((auth:), perms)")

        case "digest" =>
          if (acl.id.data.split(':').length != 2)
            throw new IllegalArgumentException(
              "ACL: digest malformed exception")

        case "ip" => ipToBytes(acl.id.data)
        case _ => throw new IllegalArgumentException(
          "ACL scheme not supported")
      }
    }
  }

  /**
   * Check IP address
   * @param addr the IP address
   * @return Array[Byte] or Exception
   */
  private[finagle] def ipToBytes(addr: String): Array[Byte] = {
    // TODO implement for ipv6

    def ipv4ToBytes(addr: String): Array[Byte] = {
      val parts = addr.split('.')
      if (parts.length != 4) {
        throw new IllegalArgumentException(
          "IP malformed exception (IP format: x.x.x.x with 0 <= x <= 255)")
      }
      val b = new Array[Byte](4)

      for (i <- 0 until 4) {
        try {
          val v: Int = parts(i).toInt
          if (v >= 0 && v <= 255) {
            b(i) = v.toByte
          }
          else throw new IllegalArgumentException(
            "IP malformed exception (IP format: x.x.x.x with 0 <= x <= 255)")
        }
        catch {
          case exc: Exception => throw new IllegalArgumentException(
            "IP malformed exception (IP format: x.x.x.x with 0 <= x <= 255)")
        }
      }
      b
    }
    ipv4ToBytes(addr)
  }


  /**
   * ACL list from a String
   * @param str a String composed by scheme:id:perm separated by commas
   * @return a Seq[ACL]
   */
  def parseACL(str: String): Seq[ACL] = {
    val aclTab = str.split(",").map(acl => acl.trim)
    val aclList: Array[ACL] = new Array[ACL](aclTab.length)

    for (i <- 0 until aclTab.length) {

      val firstColon = aclTab(i).indexOf(":")
      val lastColon = aclTab(i).lastIndexOf(":")
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon)
        throw new IllegalArgumentException(
          "does not have the form scheme:id:perm")

      val permStr = aclTab(i).substring(lastColon + 1).trim
      val schemeStr = aclTab(i).substring(0, firstColon).trim
      val dataStr = aclTab(i).substring(firstColon + 1, lastColon).trim

      if (permStr.isEmpty || schemeStr.isEmpty)
        throw new IllegalArgumentException(
          "does not have the form scheme:id:perm")
      if (!permStr.filter(char => !Set('r', 'w', 'c', 'd', 'a')
        .contains(char))
        .isEmpty)
        throw new IllegalArgumentException(
          "does not have the form scheme:id:perm")
      val newACL = new ACL(
        Perms.permFromString(permStr),
        new Id(schemeStr, dataStr))

      aclList(i) = newACL
    }
    aclList
  }

  /**
   * Permissions associated to an ACL
   */
  object Perms {
    val READ: Int = 1 << 0
    val WRITE: Int = 1 << 1
    val CREATE: Int = 1 << 2
    val DELETE: Int = 1 << 3
    val ADMIN: Int = 1 << 4
    val ALL: Int = READ | WRITE | CREATE | DELETE | ADMIN
    val CREATE_DELETE = CREATE | DELETE
    val READ_WRITE = READ | WRITE

    /**
     * To get permission from a String
     * @param permString a string composed of characters r,w,c,d,a
     * @return the permission's Int representation
     */
    def permFromString(permString: String): Int = {
      var perm: Int = 0
      permString.map {
        case 'r' => perm = perm | Perms.READ
        case 'w' => perm = perm | Perms.WRITE
        case 'c' => perm = perm | Perms.CREATE
        case 'd' => perm = perm | Perms.DELETE
        case 'a' => perm = perm | Perms.ADMIN
        case _ => throw new IllegalArgumentException(
          "this character is not supported for perms")
      }
      perm
    }
  }
}