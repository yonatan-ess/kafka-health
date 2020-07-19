import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io.StdIn
import java.lang.management.{ManagementFactory, MemoryMXBean}
import java.net.URI
import javax.management.JMX
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import spray.json._
import DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import java.lang.management.RuntimeMXBean
import javax.management.MBeanAttributeInfo
import javax.management.MBeanInfo
import javax.management.MBeanServerConnection
import scala.language.postfixOps
import javax.management.ObjectName
//import spray.json.DefaultJsonProtocol._


object KafkaStatus  {
  JMXFromLocal
  def get() = { 
    val Metrics = List(
      //"kafka.server:type=ReplicaManager,name=LeaderCount",
      "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", 
      "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"
    )
    var healthsum = 0
    var healtyness = ""
    var ReturnValues = scala.collection.mutable.Map[String,String]()
    for (metric <- Metrics) { 
      ReturnValues += (metric -> JMXFromLocal.getmetric(metric).toString)
      healthsum += JMXFromLocal.getmetric(metric).toString.toInt
      }
    if (healthsum > 0 ) { healtyness="False" } else { healtyness="True" }
    ReturnValues += ("healty" -> healtyness.toString )
    ReturnValues.toMap.toJson
  } 
} 

object JMXFromLocal {
  var jmxPeer = "msc-kfk0a.tbd.wixprod.net:9999"
  val jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://%s/jmxrmi".format(jmxPeer))
  val jmxc = JMXConnectorFactory.connect(jmxUrl, null)
  val connection = jmxc.getMBeanServerConnection
  
  def getmetric(metric: String) = {      
    connection.getAttribute(new ObjectName(metric),"Value")
  }
}

object WebServer {
  def main(args: Array[String]) = {
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val route =
      path("status") {
        get {
          complete(HttpEntity(ContentTypes.`application/json` , KafkaStatus.get().toString ))
        }
      }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
