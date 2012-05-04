/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 02/05/12
 * Time: 10:26
 * 
 */

import akka.actor._
import akka.routing.RoundRobinRouter
import com.rabbitmq.client.{Connection, Channel}
import com.typesafe.config.ConfigFactory

object BasketGenerator extends App{
  generate()

  // Message classes
  // sealed stops this trait from being used outside this module.
  // trait allows a mix of abstract and concrete methods - a mixin which allows implementation reuse.
  sealed trait SimulatorMessage
  //case classes/objects can be used for pattern matching, have compiler generated toString, equality, constructor methods.
  case object SimulationComplete extends SimulatorMessage
  case object SimulationStart extends SimulatorMessage
  case object GoShop extends SimulatorMessage
  case object ShopperDied extends SimulatorMessage
  akka.dispatch.MessageDispatcher
  class Shopper(channel: Channel, Q: String) extends Actor with ActorLogging{
    val myId: String = nextASCIIString(10)

    def receive = {
      case GoShop =>
        log.info("I'm Shopping ({})!", myId)
        // test message to Q
        val msg = ("Shopped at : " + System.currentTimeMillis());
        channel.basicPublish("", Q, null, msg.getBytes());
        // todo: generate shopper id from database
        // todo: generate basket from database
        // terminate this shopper's stint
        sender ! ShopperDied
    }

    def nextASCIIString(length: Int) = {
      val (min, max) = (33, 126)
      def nextDigit = util.Random.nextInt(max - min) + min

      new String(Array.fill(length)(nextDigit.toByte), "ASCII")
    }
  }

  /** Represents the Shop in which Shoppers will soon arrive and start shopping.
   *
   * SimulationStart event - begins Shopper activity, is sent from Controller.
   * ShopperDied event - sent from Shopper to indicate they're leaving the store.
   *
   * @param noOfShoppers
   * @param channel
   * @param Q
   * @param controller
   *
   **/

  class Shop extends Actor with ActorLogging {
    val config = ConfigFactory.load()
    val noOfShoppers = config.getInt("basketGenerator.noOfShoppers")
    val queue = config.getString("basketGenerator.rabbitmq.queue")

    // Set up RabbitMQ
    val connection = RabbitMQConnection.getConnection()
    val channel = connection.createChannel()
    channel.queueDeclare(queue, false, false, false, null)

    // We round-robin start requests to each Shopper
    val shopperRouter = context.actorOf(Props(new Shopper(channel, queue)).withRouter(RoundRobinRouter(noOfShoppers)), name = "shopperRouter")
    // Count number of shopper's leaving the store.
    var noOfShoppersDied: Int = _

    def receive = {
      case SimulationStart =>
        log.info("Start simulation of {} Shoppers. Pickup baskets at Q = {}", noOfShoppers, queue)
        for (i <- 0 until noOfShoppers) shopperRouter ! GoShop
      case ShopperDied =>
        noOfShoppersDied += 1
        if (noOfShoppersDied == noOfShoppers) {
          // Ask the 'user' generator to shutdown all its children (including Shop).
          context.system.shutdown()
        }
    }

    // Called when Actor terminates itself - having terminated its children.
    override def postStop() = {
      channel.close()
      connection.close()
    }
  }

  def generate(){
    val config = ConfigFactory.load()
    val actorSystem = ActorSystem("BasketGeneratorSystem", config)
    val shop = actorSystem.actorOf(Props[Shop], name = "topshop")

    shop ! SimulationStart
  }
}
