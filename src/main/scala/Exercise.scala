import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

object Exercise {

  def main(args: Array[String]): Unit = {

    val format = new SimpleDateFormat("dd/MMM/yyyy")

    val conf = new SparkConf().setAppName("Semantix exercise").setMaster("local")

    val sc = new SparkContext(conf)

    val files = sc.textFile("resources\\*.gz").cache()

    val hosts = files.flatMap(line => {
      val indice = line.indexOf(" - -")
      if (indice < 0) {
        line.substring(0, indice)
      } else {
        line
      }
    }).map(word => (word, 1)).reduceByKey(_ + _)

    val uniqueHosts = hosts.count()

    val error404 = files.filter(line => line.contains(" 404 ")).cache()

    val error404Total = error404.count()

    val url404 = error404.map(line => line.substring(0, line.indexOf(" ")))
      .map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    val top5Url404 = url404.take(5)

    /*
        val error404PerDay = error404.map(line =>
          {
            val format = new SimpleDateFormat("dd/MMM/yyyy", new Locale("en","US"))
            format.parse(line.substring(line.indexOf("[")+1,line.indexOf("[")+12))
          })
          .map(word => (word,1)).reduceByKey(_+_).sortBy(_._1)
    */

    val error404PerDay = error404.map(line => line.substring(line.indexOf("[") + 1, line.indexOf("[") + 12))
      .map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._1)

    val total = files.map(line => line.length()).reduce(_ + _)

    println("número de hosts únicos: " + uniqueHosts)
    println("total de erros 404: " + error404Total)
    println("top 5 url com mais erros 404: ")
    top5Url404.foreach { case (url, qtd) => println("  " + url + " -> " + qtd) }
    println("erros 404 por dia: ")
    error404PerDay.foreach { case (dia, qtd) => println("  " + dia + " -> " + qtd) }
    println("bytes retornados: " + total)

  }

}
