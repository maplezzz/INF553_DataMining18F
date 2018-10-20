import java.io.{File, PrintWriter}

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}


object ItemBasedCF {
  def main(args: Array[String]): Unit = {

    val start_time = System.nanoTime()

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("task2").setMaster("local[3]")

    val sc = new SparkContext(conf)

    //load data and build map based on trainData
    val testData = sc.textFile(args(1))
    val trainData = sc.textFile(args(0))

    val header = testData.first()
    val train = trainData.filter(_ != header).map(x => (x.split(',')(0), x.split(',')(1), x.split(',')(2).toDouble))

    //clean trainData
    val oneUser = train.map{case(user, bus, rate) => (user, 1)}
      .reduceByKey(_+_).filter(x => x._2 <= 70).map{case (user, num) => user}.collect().toList
    val oneBus = train.map{case(user, bus, rate) => (bus, 1)}
      .reduceByKey(_+_).filter(x => x._2 <= 70).map{case (bus, num) => bus}.collect.toList

    val filteredRDD = train.filter(x => !(oneUser.contains(x._1) | oneBus.contains(x._2))).cache()


    var mapUser: Map[String, Int] = filteredRDD.collect().map(x => x._1).distinct.zipWithIndex.toMap
    var mapBus: Map[String, Int] = filteredRDD.collect().map(x => x._2).distinct.zipWithIndex.toMap
    var reMapUser = mapUser.map(_.swap)
    var reMapBus = mapBus.map(_.swap)

    val testAllRDD = testData.filter(_ != header)
      .map(x => (x.split(',')(0), x.split(',')(1), x.split(',')(2).toDouble)).cache()


    val trainAllRDD = filteredRDD.map(x => (mapUser(x._1), mapBus(x._2), x._3))

    //find pairs whose user not in mapUser
    val newUserT = testAllRDD.filter{case (user, bus, rate) => !mapUser.contains(user)}
    //find pairs whose bus not in mapBus
    val newBusT = testAllRDD.filter{case (user, bus, rate) => !mapBus.contains(bus)}
    //find intersect and union
    val intersect = newBusT.intersection(newUserT)
    //update
    val newUser = newUserT.subtract(intersect)
    val newBus = newBusT.subtract(intersect)
    //find the testRDD use ASL
    val union = sc.union(newUser, newBus, intersect)
    val testRDD = testAllRDD.subtract(union).map(x => (mapUser(x._1), mapBus(x._2), x._3)).cache()



    var avg = train.map{case (user, bus, rate) => rate}.mean()
    var avg_user = train.map{case (user, bus, rate) => (user, (rate, 1))}
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(t => (t._1, t._2._1/t._2._2))
    var avg_bus = train.map{case (user, bus, rate) => (bus, (rate, 1))}
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(t => (t._1, t._2._1/t._2._2))

    var predNewUser = newUser.map{case (user, bus ,rate) => (bus, (user, rate))}.join(avg_bus)
      .map{case (bus, ((user, rate), prede)) => (user, bus, rate, prede)}

    var predNewBus = newBus.map{case (user, bus ,rate) => (user, (bus, rate))}.join(avg_user)
      .map{case (user, ((bus, rate), prede)) => (user, bus, rate, prede)}
    val preNew = intersect.map{case (user, bus, rate) => (user, bus, rate, avg)}

    //calculate item similarity
    val trainRDD = trainAllRDD.map(x => (x._1,(x._2, x._3)))
    val trainByUser = trainRDD.join(trainRDD)//.filter(x => x._2._1._1 < x._2._2._1)
      .map{case (user, ((bus1, rate1), (bus2, rate2))) => (user, (bus1, bus2, rate1, rate2))}


    val itemBasedmatrix = trainByUser.map{case (user, (bus1, bus2, rate1, rate2)) => ((bus1, bus2), (rate1, rate2))}
    val item1avg = itemBasedmatrix.map { case ((bus1, bus2), (rate1, rate2)) => ((bus1, bus2), rate1) }.groupByKey()
      .map(x => (x._1, x._2.sum / x._2.size))
    val item2avg = itemBasedmatrix.map { case ((bus1, bus2), (rate1, rate2)) => ((bus1, bus2), rate2) }.groupByKey()
      .map(x => (x._1, x._2.sum / x._2.size))

    val coAvg = item1avg.join(item2avg)

    val sub = itemBasedmatrix.join(coAvg).map{case ((bus1, bus2), ((rate1, rate2), (avg1, avg2))) => ((bus1, bus2), (rate1 - avg1, rate2 - avg2))}
    val numerator = sub.map{case ((bus1, bus2), (minus1, minus2)) => ((bus1, bus2), minus1*minus2)}.reduceByKey(_+_)
    var denom1 = sub.map{case ((bus1, bus2), (minus1, minus2)) => ((bus1, bus2), Math.pow(minus1, 2))}.reduceByKey(_+_)
    var denom2 = sub.map{case ((bus1, bus2), (minus1, minus2)) => ((bus1, bus2), Math.pow(minus2, 2))}.reduceByKey(_+_)
    var denominator = denom1.join(denom2).map{case((bus1, bus2), (de1, de2)) => ((bus1, bus2),Math.sqrt(de1)*Math.sqrt(de2))}
    var weight = numerator.join(denominator).map{case ((bus1, bus2), (num, den)) =>
        if (den == 0) ((bus1, bus2), 0.0)
        else ((bus1, bus2), num/den)
      }.cache()

    //calculate prediction using a simple weighted average
    val tRDD = testRDD.map(x => (x._1, (x._2, x._3)))
    val itemBasedTRDD = tRDD.join(tRDD)//.filter(x => x._2._1._1 < x._2._2._1)
      .map{case (user, ((bus1, rate1), (bus2, rate2))) => ((bus1, bus2), (user, rate1, rate2))}
      .join(weight)
    val predNumerator = itemBasedTRDD.map{case ((bus1, bus2), ((user, rate1, rate2), wei)) => ((user, bus1), rate2*wei)}
      .reduceByKey(_+_)
    val predDenominator = itemBasedTRDD.map{case ((bus1, bus2), ((user, rate1, rate2), wei)) => ((user, bus1), Math.abs(wei))}
      .reduceByKey(_+_)
    val pred = predNumerator.join(predDenominator).map{case  ((user, bus1), (num, den)) =>
        if (den == 0) ((user, bus1), 1.0)
        else {
          if (num/den < 1.0) ((user, bus1), 1.0)
          else if (num/den >5.0) ((user, bus1), 5.0)
          else ((user, bus1), num/den)
        }
    }
    val prediction = testRDD.map(x => ((x._1, x._2), x._3)).join(pred)
      .map{case ((user, bus),(rate, prede)) => (reMapUser(user), reMapBus(bus), rate, prede)}


    val out = sc.union(predNewBus, predNewUser, preNew, prediction).cache()

    val MSE = out.map { case (user, bus, r1, r2) =>
      val err = r1 - r2
      err * err
    }.mean()

    val RMSE = Math.sqrt(MSE)
    val output = out.sortBy(r => (r._1, r._2)).map{case (user, bus, rate, prede) => (user, bus, prede)}

    val text = new PrintWriter(
      new File( "Fengyu_Zhang_ItemBasedCF.txt")
    )


    output.collect().foreach( value => text.write(value._1 + ", " + value._2 + ", " + value._3 + "\n"))

    text.close()

    val accum01 = sc.accumulator(0)
    val accum12 = sc.accumulator(0)
    val accum23 = sc.accumulator(0)
    val accum34 = sc.accumulator(0)
    val acccum4x = sc.accumulator(0)
    out.foreach(error => {
      val err = Math.abs(error._3 - error._4)
      if (err < 1 && err >= 0) {
        accum01 += 1
      } else if (err < 2 && err >= 1) {
        accum12 += 1
      } else if (err < 3 && err >= 2) {
        accum23 += 1
      } else if (err < 4 && err >= 3) {
        accum34 += 1
      } else {
        acccum4x += 1
      }
    })

    println(">= 0 and < 1: " + accum01)
    println(">= 1 and < 2: " + accum12)
    println(">= 2 and < 3: " + accum23)
    println(">= 3 and < 4: " + accum34)
    println(">= 4: " + acccum4x)
    println(s"RMSE: $RMSE")


    println(output.count(), testAllRDD.count())

    val end_time = System.nanoTime()
    val time = (end_time - start_time) / 1000000000
    println("Time: " + time + " sec")
    sc.stop()






  }
}
