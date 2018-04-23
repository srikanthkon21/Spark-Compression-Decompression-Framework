package default

import scala.concurrent.Future
import org.apache.hadoop.io.compress.GzipCodec

//import com.databricks.spark.avro._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.cli._;
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.collection.mutable.Stack
import org.apache.hadoop.io.compress.GzipCodec




object Compression {
def main(args : Array[String]): Unit={

    println(args(0))
    println(args(1))

    val landing_zone=args(0)
    val input_dir=args(1)
    var filter_extension=".bz,.gz"
    var minsize=0L

    var dirs = Stack[String]()
    val files = scala.collection.mutable.ListBuffer.empty[String]
    val conf = new SparkConf().setAppName("Compression"+args(0))
		val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)

    dirs.push(input_dir)

    while(!dirs.isEmpty){
        val status = fs.listStatus(new Path(dirs.pop()))
        status.foreach(x=> if(x.isDirectory) dirs.push(x.getPath.toString)
          else if(x.getLen>minsize) files+= x.getPath.toString)
    }

    val filtered_files = files.filter(a=>if (filter_extension.contains(a.splitAt(a.lastIndexOf("."))._2 )) false else true)

  	try{
      filtered_files.foreach{
                  filename=>{
                        if(fs.exists(new Path(filename))){
                          val rdd = sc.textFile(filename)
                          val output_dir = landing_zone+filename.splitAt(filename.lastIndexOf("/"))._2
                              rdd.coalesce(1).saveAsTextFile(output_dir, classOf[GzipCodec])
							  if(fs.exists(new Path(output_dir)))
							  fs.delete(new Path(output_dir))
                          var tmp_compressed_path=output_dir+"/part-00000.gz"
                              if(fs.rename(new Path(tmp_compressed_path),new Path(filename+".gz"))){
                                          fs.delete(new Path(filename),false)
                              }
                              fs.delete(new Path(output_dir),true)
							  fs.delete(new Path(landing_zone),true)
                        }

                  }
        }


  	}  finally{
  		sc.stop()
  	   }
	}

}
