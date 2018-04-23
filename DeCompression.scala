package default

import scala.concurrent.Future
import org.apache.hadoop.io.compress.GzipCodec

//import com.databricks.spark.avro._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.cli._;
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.collection.mutable.Stack
import org.apache.hadoop.io.compress.GzipCodec



object DeCompression {
  def main(args : Array[String]): Unit={

      println(args(0))
      println(args(1))
     // println(args(2))

      val landing_zone=args(0)
      val input_dir=args(1)
      var filter_extension=".bz,.gz"
      var minsize=100L

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

      val filtered_files = files.filter(a=>if (filter_extension.contains(a.splitAt(a.lastIndexOf("."))._2 )) true else false)

    	try{
          filtered_files.foreach{
		              filename=>{
		                           if(fs.exists(new Path(filename))){
                                  
                                                  val rdd = sc.textFile(filename)
                                                  val output_dir = landing_zone+filename.splitAt(filename.lastIndexOf("/"))._2
                                                  rdd.repartition(1).saveAsTextFile(output_dir)
												  if(fs.exists(new Path(output_dir)))
												  fs.delete(new Path(output_dir))
                                                  var tmp_output_path=output_dir+"/part-00000"
                                                  if(fs.rename(new Path(tmp_output_path),new Path(filename.substring(0,filename.size-3)))){
                                                                  fs.delete(new Path(filename),false)
                                                  }
                                                  fs.delete(new Path(output_dir))
												  fs.delete(new Path(landing_zone),true)
												  
												 
                                  }
                  }
				}  

    	}  finally{
    		sc.stop()
    	   }
  	}

  }
