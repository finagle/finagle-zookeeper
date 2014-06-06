package com.twitter.finagle.exp.zookeeper.utils

import com.twitter.finagle.exp.zookeeper.ZookeeperDefs.CreateMode

object PathUtils {

  def validatePath(path: String, createMod: Int): Unit = {
    if (createMod == 0 || createMod == 1 || createMod == 2 || createMod == 0) {
      if (createMod == CreateMode.EPHEMERAL_SEQUENTIAL || createMod == CreateMode.PERSISTENT_SEQUENTIAL)
        validatePath(path + 1)
      else
        validatePath(path)
    } else throw new IllegalArgumentException("Create mode is not correct")
  }

  def validatePath(path: String): Unit = {
    if (path == null) {
      throw new IllegalArgumentException("Path cannot be null")
    }
    if (path.length == 0) {
      throw new IllegalArgumentException("Path length must be > 0")
    }
    if (path.charAt(0) != '/') {
      throw new IllegalArgumentException("Path must start with / character")
    }
    if (path.length == 1) {
      return
    }
    if (path.charAt(path.length - 1) == '/') {
      throw new IllegalArgumentException("Path must not end with / character")
    }

    var reason: String = ""
    var lastc: Char = '/'
    val chars = path.replaceFirst("/", "").toList
    var index = 0

    for (char <- chars) {
      if (char == 0) {
        reason = "null character not allowed @" + index
        throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason)
      }
      else if (char == '/' && lastc == '/') {
        reason = "empty node name specified @" + index
        throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason)
      }
      else if (char == '.' && lastc == '.') {
        if (chars(index - 2) == '/' && ((index + 1 == chars.length) || chars(index + 1) == '/')) {
          reason = "relative paths not allowed @" + index
          throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason)
        }
      }
      else if (char == '.') {
        if (chars(index - 1) == '/' && ((index + 1 == chars.length) || chars(index + 1) == '/')) {
          reason = "relative paths not allowed @" + index
          throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason)
        }
      }
      else if (char > '\u0000' && char < '\u001f' || char > '\u007f' && char < '\u009F' || char > '\ud800' && char < '\uf8ff' || char > '\ufff0' && char < '\uffff') {
        reason = "invalid character @" + index
        throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason)
      }
      lastc = char
      index += 1
    }
  }

  def prependChroot(clientPath: String, chRootPath: String): String = {
    // TODO check and test
    if (chRootPath != null) {
      // handle clientPath = "/"
      if (clientPath.length() == 1) {
        chRootPath
      }
      chRootPath + clientPath
    } else {
      clientPath
    }
  }

}
