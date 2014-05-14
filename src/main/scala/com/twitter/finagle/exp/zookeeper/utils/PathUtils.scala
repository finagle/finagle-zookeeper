package com.twitter.finagle.exp.zookeeper.utils

import com.twitter.finagle.exp.zookeeper.ZookeeperDefinitions.createMode

object PathUtils {

  /**
   * Validate the provided znode path string
   * @param path znode path string
   * @throws IllegalArgumentException if the path is invalid
   *
   */

  /**
   * TO CHECK AND TEST
   */

  def validatePath(path: String, createMod: Int) = {
    if (createMod == createMode.EPHEMERAL_SEQUENTIAL || createMod == createMode.PERSISTENT_SEQUENTIAL)
      path + 1
    else
      path
  }


  def validatePath(path: String) {
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
    var reason: String = null
    var lastc: Char = '/'
    val chars = path.toList
    var index = 0

    chars.map {
      char => {
        if (char == 0) {
          reason = "null character not allowed @" + index
        }
        else if (char == '/' && lastc == '/') {
          reason = "empty node name specified @" + index
        }
        else if (char == '.' && lastc == '.') {
          if (chars(index - 2) == '/' && ((index + 1 == chars.length) || chars(index + 1) == '/')) {
            reason = "relative paths not allowed @" + index
          }
        }
        else if (char == '.') {
          if (chars(index - 1) == '/' && ((index + 1 == chars.length) || chars(index + 1) == '/')) {
            reason = "relative paths not allowed @" + index
          }
        }
        else if (char > '\u0000' && char < '\u001f' || char > '\u007f' && char < '\u009F' || char > '\ud800' && char < '\uf8ff' || char > '\ufff0' && char < '\uffff') {
          reason = "invalid charater @" + index
        }
        index += 1
      }
    }

    if (reason != null) {
      throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason)
    }
  }

  def prependChroot(clientPath: String, chRootPath: String): String = {
    if (chRootPath != null) {
      // handle clientPath = "/"
      if (clientPath.length() == 1) {
        chRootPath;
      }
      chRootPath + clientPath;
    } else {
      clientPath;
    }
  }

}
