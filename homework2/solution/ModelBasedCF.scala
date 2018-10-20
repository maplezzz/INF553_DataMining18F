import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import java.io.{File, PrintWriter}



object ModelBasedCF {


  def main(args: Array[String]): Unit = {

    val start_time = System.nanoTime()

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("task1").setMaster("local[2]")

    val sc = new SparkContext(conf)

    //load data and build map based on trainData
    val testData = sc.textFile(args(1))
    val trainData = sc.textFile(args(0))

    val header = testData.first()
    val train = trainData.filter(_ != header).cache()

    val testAllRDD = testData.filter(_ != header)
      .map(x => (x.split(',')(0), x.split(',')(1), x.split(',')(2).toDouble)).cache()

    val trainAlRDD = train.map(x => (x.split(',')(0), x.split(',')(1), x.split(',')(2).toDouble)).cache()

    //clean train, delete one user and one bus
    val oneUser = trainAlRDD.map{case(user, bus, rate) => (user, 1)}
      .reduceByKey(_+_).filter(x => x._2 <= 3).map{case (user, num) => user}.collect().toList
    val oneBus = trainAlRDD.map{case(user, bus, rate) => (bus, 1)}
      .reduceByKey(_+_).filter(x => x._2 <= 3).map{case (bus, num) => bus}.collect.toList

    val trainAllRDD = trainAlRDD.filter(x => !(oneUser.contains(x._1) | oneBus.contains(x._2))).cache()

    var mapUser: Map[String, Int] = trainAllRDD.collect().map(x => x._1).distinct.zipWithIndex.toMap
    var mapBus: Map[String, Int] = trainAllRDD.collect().map(x => x._2).distinct.zipWithIndex.toMap
    var reMapUser = mapUser.map(_.swap)
    var reMapBus = mapBus.map(_.swap)

    val trainRDD = trainAllRDD.map{case (user, bus, rate) => Rating(mapUser(user), mapBus(bus), rate)}

    //find pairs whose user not in mapUser
    val newUserT = testAllRDD.filter{case (user, bus, rate) => !mapUser.contains(user)}
    //find pairs whose bus not in mapBus
    val newBusT = testAllRDD.filter{case (user, bus, rate) => !mapBus.contains(bus)}
    //find intersect and union
    val intersect = newBusT.intersection(newUserT).cache()
    //update
    val newUser = newUserT.subtract(intersect).cache()
    val newBus = newBusT.subtract(intersect).cache()
    //find the testRDD use ASL
    val union = sc.union(newUser, newBus, intersect)
    val testRDD = testAllRDD.subtract(union).map{case (user, bus, rate) => (mapUser(user), mapBus(bus), rate)}

    //find avg rating all data, user, bus
    var avg = trainAllRDD.map{case (user, bus, rate) => rate}.mean()
    var avg_user = trainAllRDD.map{case (user, bus, rate) => (user, (rate, 1))}
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(t => (t._1, t._2._1/t._2._2))
    var avg_bus = trainAllRDD.map{case (user, bus, rate) => (bus, (rate, 1))}
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).map(t => (t._1, t._2._1/t._2._2))

    var predNewUser = newUser.map{case (user, bus ,rate) => (bus, (user, rate))}.join(avg_bus)
      .map{case (bus, ((user, rate), pred)) => (user, bus, rate, pred)}
    var predNewBus = newBus.map{case (user, bus ,rate) => (user, (bus, rate))}.join(avg_user)
      .map{case (user, ((bus, rate), pred)) => (user, bus, rate, pred)}
    val preNew = intersect.map{case (user, bus, rate) => (user, bus, rate, avg)}


    //predict testRdd use ASL
    var rank = 3
    var numIterations = 25
    var model = ALS.train(trainRDD, rank, numIterations, 0.3)

    val tRDD = testRDD.map{
      case (user, bus, rate) => (user, bus)
    }

    val preditRDD = model.predict(tRDD).map{
      case Rating(user, bus, rate) => ((user, bus), rate)
      }

    val validRDD = testRDD.map{
      case (user, bus, rate) => ((user, bus), rate)
    }


    val ratesAndPreds = validRDD.join(preditRDD).map{case ((user, bus),(rate, pred)) => (reMapUser(user), reMapBus(bus), rate, pred)}


    val out = sc.union(predNewBus, predNewUser, preNew, ratesAndPreds).cache()

    val MSE = out.map { case (user, bus, r1, r2) =>
      val err = r1 - r2
      err * err
    }.mean()

    val RMSE = Math.sqrt(MSE)
    val output = out.sortBy(r => (r._1, r._2)).map{case (user, bus, rate, pred) => (user, bus, pred)}

    val text = new PrintWriter(
     new File( "Fengyu_Zhang_ModelBasedCF.txt")
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

    val end_time = System.nanoTime()
    val time = (end_time - start_time) / 1000000000
    println("Time: " + time + " sec")
    sc.stop()
  }
}