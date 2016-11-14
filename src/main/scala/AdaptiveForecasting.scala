import java.io.PrintStream
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 12-Nov-16.
  */
object AdaptiveForecasting {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Adaptive Forecasting").set("spark.driver.memory","6g").set("spark.executor.memory","6g")
    val sc=new SparkContext(sparkConf)
    val dataset=sc.textFile("G:\\Dropbox\\2016 Fall\\Network Analytics\\Network Analytics-Assignment\\Assignment\\Assignment 3\\Dataset3.csv")
    val groupedDataset=dataset.map(f=>{
      val s=f.split(";")
      val sdf = new SimpleDateFormat("M/dd/yyyy hh:mm:ss a")
      val date = sdf.parse(s(1))
      (s(0),date,s(2))
    }).sortBy(f=>f._2).groupBy(f=>f._3)

    val result=groupedDataset.map(f=>{
      val ff=f._2.toArray
      val sInt=ff.map(f=> java.lang.Double.parseDouble(f._1).toDouble)
      var j=0
      var smoothing=0.0 to 1.0 by 0.1
      var pre=new Array[Double](smoothing.length)
      for (i <- 0 to 9)
        {
          pre(i)=sInt(0)
        }
      val sFF=ff.map(sf=>{
        var sma=0.0
        var wma=0.0
        var exp=new Array[Double](smoothing.length)

        if(j<10||j>ff.length) {
          sma = 0.0
          wma=0.0
        }
        else {
         sma=SimpleAvg(10, sInt, j)
         wma=WeightedAvg(10,sInt,j)
        }
        j = j + 1
        (sma,wma)
      })
      j=0
      val ssFF=ff.map(sf=>{
        var exp=new Array[Double](smoothing.length)

        if(j<1||j>ff.length) {

        }
        else {
          var d=0
          exp=smoothing.map(f=>{
          val s=Exponential(pre(d),sInt,j,f)
          //println(s+":"+pre(d))
          d=d+1
          s
          }).toArray
          pre=exp
          println(exp.mkString(";"))
        }
        j = j + 1
        (exp.mkString(";"))
      })
      val r=ff.zip(sFF)
      r.map(f=>{
        (f._1._1,f._1._2,f._1._3,f._2._1,f._2._2)
      }).zip(ssFF)
      })
    val output=new PrintStream("G:\\Dropbox\\2016 Fall\\Network Analytics\\Network Analytics-Assignment\\Assignment\\Assignment 3\\SMA.csv")
    output.println("OriginalData;Date;DatasetName;SMA;WMA;EXP-0.0;EXP-0.1;EXP-0.2;EXP-0.3;EXP-0.4;EXP-0.5;EXP-0.6;EXP-0.7;EXP-0.8;EXP-0.9;EXP-1.0")
    result.collect().foreach(f=>{
      f.foreach(ff=>{
          output.println(ff._1._1+";"+ff._1._2+";"+ff._1._3+";"+ff._1._4+";"+ff._1._5+";"+ff._2)
        })
    })
/*    result.flatMap(f=>{
     f._2
    }).foreach(println(_))*//*.map(f=>{
      f._2.foreach(ff=>{
        output.println(f._1+";"+ff._1._1+";"+ff._1._2+";"+ff._1._3+";"+ff._2._1+";"+ff._2._2+";"+ff._2._3)
      })
    })*/
    //result.saveAsTextFile("G:\\Dropbox\\2016 Fall\\Network Analytics\\Network Analytics-Assignment\\Assignment\\Assignment 3\\Result")
  }

  def SimpleAvg(dataP:Int,ss:Array[Double],index:Int): Double =
  {
    var sum=0.0
    var avg=0.0
    for (i<- dataP to 0 by -1){
      sum+=ss(index-i)
    }
    avg=sum/dataP
    avg
  }

  def WeightedAvg(dataP:Int,ss:Array[Double],index:Int): Double =
  {
    var sum=0.0
    var avg=0.0
    val weig=1 to 10
    val s=weig.sum
    var jj=0
    for (i<- dataP to 1 by -1){
      sum+=weig(jj)*ss(index-i)/s
      jj=jj+1
    }
    avg=sum
    avg
  }

  def Exponential(previous:Double,ss:Array[Double],index:Int,smoothing:Double): Double =
  {
    var forecast=0.0
    forecast=smoothing*ss(index-1)+(1-smoothing)*previous
    forecast
  }

 /* def Holtz(dataP:Int,previous:Double,ss:Array[Double],index:Int,alpha:Double,beta:Double): Double =
  {
    var forecast=0.0

    var level= alpha*
    //println(s)
    forecast=smoothing*ss(index-1)+(1-smoothing)*previous

    forecast
  }*/


}
