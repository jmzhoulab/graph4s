package mu.atlas.graph.utils

import java.io.{FileSystem => _, _}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

/**
  * Created by zhoujiamu on 2017/8/23.
  */
object HdfsHelper {

    private val hdfs : FileSystem = FileSystem.get(new Configuration)

    def isDir(name: String): Boolean = hdfs.isDirectory(new Path(name))

    def isDir(path: Path): Boolean = hdfs.isDirectory(path)

    def isDir(hdfs : FileSystem, name : String) : Boolean = hdfs.isDirectory(new Path(name))

    def isDir(hdfs : FileSystem, name : Path) : Boolean = hdfs.isDirectory(name)

    def isFile(hdfs : FileSystem, name : String) : Boolean = hdfs.isFile(new Path(name))

    def isFile(hdfs : FileSystem, name : Path) : Boolean = hdfs.isFile(name)

    def createFile(hdfs : FileSystem, name : String) : Boolean = hdfs.createNewFile(new Path(name))

    def createFile(name : Path) : Boolean = hdfs.createNewFile(name)

    def createFolder(name : String) : Boolean = hdfs.mkdirs(new Path(name))

    def createFolder(name : Path) : Boolean = hdfs.mkdirs(name)

    def exists(name : String) : Boolean = hdfs.exists(new Path(name))

    def exists(name : Path) : Boolean = hdfs.exists(name)

    def rename(oldName : String, NewName: String) : Boolean = hdfs.rename(new Path(oldName), new Path(NewName))

    def rename(oldName : Path, NewName: String) : Boolean = hdfs.rename(oldName, new Path(NewName))

    def rename(oldName : String, NewName: Path) : Boolean = hdfs.rename(new Path(oldName), NewName)

    def rename(oldName : Path, NewName: Path) : Boolean = hdfs.rename(oldName, NewName)
    
    def transport(inputStream : InputStream, outputStream : OutputStream): Unit ={
        val buffer = new Array[Byte](64 * 1000)
        var len = inputStream.read(buffer)
        while (len != -1) {
            outputStream.write(buffer, 0, len - 1)
            len = inputStream.read(buffer)
        }
        outputStream.flush()
        inputStream.close()
        outputStream.close()
    }

    class MyPathFilter extends PathFilter {
        override def accept(path: Path): Boolean = true
    }

    /**
      * create a target file and provide parent folder if necessary
      */
    def createLocalFile(fullName : String) : File = {
        val target : File = new File(fullName)
        if(!target.exists){
            val index = fullName.lastIndexOf(File.separator)
            val parentFullName = fullName.substring(0, index)
            val parent : File = new File(parentFullName)

            if(!parent.exists)
                parent.mkdirs
            else if(!parent.isDirectory)
                parent.mkdir

            target.createNewFile
        }
        target
    }

    /**
      * delete file in hdfs
      * @return true: success, false: failed
      */
    def deleteFile(hdfs : FileSystem, path: String) : Boolean = {
        if (isDir(hdfs, path))
            hdfs.delete(new Path(path), true)   //true: delete files recursively
        else
            hdfs.delete(new Path(path), false)
    }

    /**
      * get all file children's full name of a hdfs dir, not include dir children
      * @param fullName the hdfs dir's full name
      */
    def listChildren(hdfs : FileSystem, fullName : String, holder : ListBuffer[String]) : ListBuffer[String] = {
        val filesStatus = hdfs.listStatus(new Path(fullName), new MyPathFilter)
        for(status <- filesStatus){
            val filePath : Path = status.getPath
            if(isFile(hdfs,filePath))
                holder += filePath.toString
            else
                listChildren(hdfs, filePath.toString, holder)
        }
        holder
    }

    def copyFile(hdfs : FileSystem, source: String, target: String): Unit = {

        val sourcePath = new Path(source)
        val targetPath = new Path(target)

        if(!exists(targetPath))
            createFile(targetPath)

        val inputStream : FSDataInputStream = hdfs.open(sourcePath)
        val outputStream : FSDataOutputStream = hdfs.create(targetPath)
        transport(inputStream, outputStream)
    }

    def copyFolder(hdfs : FileSystem, sourceFolder: String, targetFolder: String): Unit = {
        val holder : ListBuffer[String] = new ListBuffer[String]
        val children : List[String] = listChildren(hdfs, sourceFolder, holder).toList
        for(child <- children)
            copyFile(hdfs, child, child.replaceFirst(sourceFolder, targetFolder))
    }

    def copyFileFromLocal(hdfs : FileSystem, localSource: String, hdfsTarget: String): Unit = {
        val targetPath = new Path(hdfsTarget)
        if(!exists(targetPath))
            createFile(targetPath)

        val inputStream : FileInputStream = new FileInputStream(localSource)
        val outputStream : FSDataOutputStream = hdfs.create(targetPath)
        transport(inputStream, outputStream)
    }

    def copyFileToLocal(hdfs : FileSystem, hdfsSource: String, localTarget: String): Unit = {
        val localFile : File = createLocalFile(localTarget)

        val inputStream : FSDataInputStream = hdfs.open(new Path(hdfsSource))
        val outputStream : FileOutputStream = new FileOutputStream(localFile)
        transport(inputStream, outputStream)
    }

    def copyFolderFromLocal(hdfs : FileSystem, localSource: String, hdfsTarget: String): Unit = {
        val localFolder : File = new File(localSource)
        val allChildren : Array[File] = localFolder.listFiles
        for(child <- allChildren){
            val fullName = child.getAbsolutePath
            val nameExcludeSource : String = fullName.substring(localSource.length)
            val targetFileFullName : String = hdfsTarget + Path.SEPARATOR + nameExcludeSource
            if(child.isFile)
                copyFileFromLocal(hdfs, fullName, targetFileFullName)
            else
                copyFolderFromLocal(hdfs, fullName, targetFileFullName)
        }
    }

    def copyFolderToLocal(hdfs : FileSystem, hdfsSource: String, localTarget: String): Unit = {
        val holder : ListBuffer[String] = new ListBuffer[String]
        val children : List[String] = listChildren(hdfs, hdfsSource, holder).toList
        val hdfsSourceFullName = hdfs.getFileStatus(new Path(hdfsSource)).getPath.toString
        val index = hdfsSourceFullName.length
        for(child <- children){
            val nameExcludeSource : String = child.substring(index + 1)
            val targetFileFullName : String = localTarget + File.separator + nameExcludeSource
            copyFileToLocal(hdfs, child, targetFileFullName)
        }
    }
}
