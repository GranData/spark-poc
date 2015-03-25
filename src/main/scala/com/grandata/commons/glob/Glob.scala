//package com.grandata.commons.glob
//
//import java.io.File
//import java.nio.file.{Paths, FileSystems}
//
//import scala.collection.JavaConverters._
//
///**
// * Created by gustavo on 25/03/15.
// */
//object Glob {
//  def splitPath(path: String) = {
//    val (dirname, basename) = path.splitAt(path.lastIndexOf("/"))
//    (dirname, basename.tail)
//  }
//
//  def globInDir(dirname: String, pattern: String) = {
//    val file = if (dirname.isEmpty) {
//      new File(".")
//    } else {
//      new File(dirname)
//    }
//  }
//  if not dirname:
//    dirname = os.curdir
//  if isinstance(pattern, _unicode) and not isinstance(dirname, unicode):
//    dirname = unicode(dirname, sys.getfilesystemencoding() or
//    sys.getdefaultencoding())
//  try:
//  names = os.listdir(dirname)
//  except os.error:
//  return []
//  if pattern[0] != '.':
//  names = filter(lambda x: x[0] != '.', names)
//  return fnmatch.filter(names, pattern)
//
//
//  def glob(pattern: String): Stream[File] = {
//    val matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern)
//
//    def iglob(pattern: String) = {
//      val (dirname, basename) = splitPath(pattern)
//      val dirs = if (dirname != pattern) glob(dirname) else List(dirname)
//
//      dirs.map { dir =>
//        globInDir(dirname, basename).map { name =>
//          dirname = dirname + "/" + name
//        }
//      }
//      val dirs = iglob(patterns.tail)
//      // /*/*
//      patterns.map { pattern =>
//        Option(baseDir.listFiles())
//      }
//
//      def getFileTree(f: File): Stream[File] =
//        f #:: if (matcher.matches(Paths.get(f.getPath))) if (f.isDirectory) Option(f.listFiles()).map(_.toStream.flatMap(getFileTree)).getOrElse(Stream.empty)
//      else Stream.empty
//
//      getFileTree(f)
//    }
//
//    iglob(pattern)
//  }
//}
