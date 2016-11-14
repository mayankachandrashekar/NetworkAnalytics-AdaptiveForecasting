import java.io.PrintStream

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 12-Nov-16.
  */
object AdaptiveDynamicForecasting2 {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Adaptive Forecasting").set("spark.driver.memory","6g").set("spark.executor.memory","6g")
    val sc=new SparkContext(sparkConf)
    val dataset=sc.textFile("G:\\Dropbox\\2016 Fall\\Network Analytics\\Network Analytics-Assignment\\Assignment\\Assignment 3\\SMA.csv")
    val groupedDataset=dataset.map(f=>{
      val s=f.split(";")
       (s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15))
    }).sortBy(f=>f._2).groupBy(f=>f._3)

    val result=groupedDataset.map(f=>{
      val ff=f._2.toArray
      var initial=ff(0)._2
      var j = -2
      val dataset=f._1
      var result=""
      var choosenModel=""
      var model=List("SMA","WMA","EXP-0.0","EXP-0.1","EXP-0.2","EXP-0.3","EXP-0.4","EXP-0.5","EXP-0.6","EXP-0.7","EXP-0.8","EXP-0.9","EXP-1.0")
      var original=new Array[Double](5)
      var forecast=new Array[(Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double)](5)
      var initialTime=""
      ff.foreach(f=>{
        if(j<5&&j>0){
          original(j)= java.lang.Double.parseDouble(f._1)
          forecast(j)= (java.lang.Double.parseDouble(f._4),java.lang.Double.parseDouble(f._5),java.lang.Double.parseDouble(f._6),java.lang.Double.parseDouble(f._7),java.lang.Double.parseDouble(f._8),java.lang.Double.parseDouble(f._9),java.lang.Double.parseDouble(f._10),java.lang.Double.parseDouble(f._11),java.lang.Double.parseDouble(f._12),java.lang.Double.parseDouble(f._13),java.lang.Double.parseDouble(f._14),java.lang.Double.parseDouble(f._15),java.lang.Double.parseDouble(f._16))
        }
        else {
          if (j == 5) {
            var error = MAPE(original, forecast)
            var minError = 10000.0
            var index = 9000
            var k = 0
            error(2)=900000
            error.foreach(f => {
              if (minError > f) {
                minError = f
                index = k
              }
              k = k + 1
            })
            choosenModel = model(index)
            result += "\n" + dataset + ";" + initialTime + ";" + choosenModel + ";" + minError + ";" + error.mkString(";") + ";"
          }
          else {
            if (j < 25 && j > 0) {
              if (j == 24) {
                result += f._2.toString
                j = -2
              }
            }
            else {
              if (j == 0) {
                initialTime = f._2.toString
                original(j)= java.lang.Double.parseDouble(f._1)
                forecast(j)= (java.lang.Double.parseDouble(f._4),java.lang.Double.parseDouble(f._5),java.lang.Double.parseDouble(f._6),java.lang.Double.parseDouble(f._7),java.lang.Double.parseDouble(f._8),java.lang.Double.parseDouble(f._9),java.lang.Double.parseDouble(f._10),java.lang.Double.parseDouble(f._11),java.lang.Double.parseDouble(f._12),java.lang.Double.parseDouble(f._13),java.lang.Double.parseDouble(f._14),java.lang.Double.parseDouble(f._15),java.lang.Double.parseDouble(f._16))
              }
            }
          }
        }
        j=j+1
      })
      result
    })
    val out=new PrintStream("G:\\Dropbox\\2016 Fall\\Network Analytics\\Network Analytics-Assignment\\Assignment\\Assignment 3\\AdaptiveResultMAPE2.csv")
    result.collect().foreach(f=>{
        out.println(f)
    })

  }

 def MAPE(original:Array[Double],forecasted:Array[(Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double)]): Array[Double] ={

   var sumOfDiff=Array.fill[Double](13)(0)
   val error = new Array[Double](13)
   for (i<- 0 to 4){
     sumOfDiff(0)+=Math.abs(original(i)-forecasted(i)._1)/Math.abs(original(i))
   }
   error(0)=sumOfDiff(0)*100/10
   for (i<- 0 to 4){
     sumOfDiff(1)+=Math.abs(original(i)-forecasted(i)._2)/Math.abs(original(i))
   }
   error(1)=sumOfDiff(1)*100/10
   for (i<- 0 to 4){
     sumOfDiff(2)+=Math.abs(original(i)-forecasted(i)._3)/Math.abs(original(i))
   }
   error(2)=sumOfDiff(2)*100/10
   for (i<- 0 to 4){
     sumOfDiff(3)+=Math.abs(original(i)-forecasted(i)._4)/Math.abs(original(i))
   }
   error(3)=sumOfDiff(3)*100/10
   for (i<- 0 to 4){
     sumOfDiff(4)+=Math.abs(original(i)-forecasted(i)._5)/Math.abs(original(i))
   }
   error(4)=sumOfDiff(4)*100/10
   for (i<- 0 to 4){
     sumOfDiff(5)+=Math.abs(original(i)-forecasted(i)._6)/Math.abs(original(i))
   }
   error(5)=sumOfDiff(5)*100/10
   for (i<- 0 to 4){
     sumOfDiff(6)+=Math.abs(original(i)-forecasted(i)._7)/Math.abs(original(i))
   }
   error(6)=sumOfDiff(6)*100/10
   for (i<- 0 to 4){
     sumOfDiff(7)+=Math.abs(original(i)-forecasted(i)._8)/Math.abs(original(i))
   }
   error(7)=sumOfDiff(7)*100/10
   for (i<- 0 to 4){
     sumOfDiff(8)+=Math.abs(original(i)-forecasted(i)._9)/Math.abs(original(i))
   }
   error(8)=sumOfDiff(8)*100/10
   for (i<- 0 to 4){
     sumOfDiff(9)+=Math.abs(original(i)-forecasted(i)._10)/Math.abs(original(i))
   }
   error(9)=sumOfDiff(9)*100/10
   for (i<- 0 to 4){
     sumOfDiff(10)+=Math.abs(original(i)-forecasted(i)._11)/Math.abs(original(i))
   }
   error(10)=sumOfDiff(10)*100/10
   for (i<- 0 to 4){
     sumOfDiff(11)+=Math.abs(original(i)-forecasted(i)._12)/Math.abs(original(i))
   }
   error(11)=sumOfDiff(11)*100/10
   for (i<- 0 to 4){
     sumOfDiff(12)+=Math.abs(original(i)-forecasted(i)._13)/Math.abs(original(i))
   }
   error(12)=sumOfDiff(12)*100/10

   error
 }
  def MAD(original:Array[Double],forecasted:Array[(Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double)]): Array[Double] ={

    var sumOfDiff=Array.fill[Double](13)(0.0)
    sumOfDiff.foreach(println(_))
    println()
    println("\n ORIGINALA"+original.mkString(" "))
    println("\nFORECASTEDDDD"+forecasted.mkString("\n"))
    println(original.length)
    println(forecasted.length)
    val error = new Array[Double](13)
    for (i<- 0 to 4){
      sumOfDiff(0)+=Math.abs(original(i)-forecasted(i)._1)
    }
    error(0)=sumOfDiff(0)/10
    for (i<- 0 to 4){
      sumOfDiff(1)+=Math.abs(original(i)-forecasted(i)._2)
    }
    error(1)=sumOfDiff(1)/10
    for (i<- 0 to 4){
      sumOfDiff(2)+=Math.abs(original(i)-forecasted(i)._3)
    }
    error(2)=sumOfDiff(2)/10
    for (i<- 0 to 4){
      sumOfDiff(3)+=Math.abs(original(i)-forecasted(i)._4)
    }
    error(3)=sumOfDiff(3)/10
    for (i<- 0 to 4){
      sumOfDiff(4)+=Math.abs(original(i)-forecasted(i)._5)
    }
    error(4)=sumOfDiff(4)/10
    for (i<- 0 to 4){
      sumOfDiff(5)+=Math.abs(original(i)-forecasted(i)._6)
    }
    error(5)=sumOfDiff(5)/10
    for (i<- 0 to 4){
      sumOfDiff(6)+=Math.abs(original(i)-forecasted(i)._7)
    }
    error(6)=sumOfDiff(6)/10
    for (i<- 0 to 4){
      sumOfDiff(7)+=Math.abs(original(i)-forecasted(i)._8)
    }
    error(7)=sumOfDiff(7)/10
    for (i<- 0 to 4){
      sumOfDiff(8)+=Math.abs(original(i)-forecasted(i)._9)
    }
    error(8)=sumOfDiff(8)/10
    for (i<- 0 to 4){
      sumOfDiff(9)+=Math.abs(original(i)-forecasted(i)._10)
    }
    error(9)=sumOfDiff(9)/10
    for (i<- 0 to 4){
      sumOfDiff(10)+=Math.abs(original(i)-forecasted(i)._11)
    }
    error(10)=sumOfDiff(10)/10
    for (i<- 0 to 4){
      sumOfDiff(11)+=Math.abs(original(i)-forecasted(i)._12)
    }
    error(11)=sumOfDiff(11)/10
    for (i<- 0 to 4){
      sumOfDiff(12)+=Math.abs(original(i)-forecasted(i)._13)
    }
    error(12)=sumOfDiff(12)/10

    error

 }


}
