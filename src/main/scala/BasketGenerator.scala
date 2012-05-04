/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 02/05/12
 * Time: 10:26
 * 
 */

import com.rabbitmq.client.Channel
import akka.actor._
import akka.routing.RoundRobinRouter
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

  class Shop(noOfShoppers: Int, channel: Channel, Q: String, controller: ActorRef) extends Actor with ActorLogging {
    // We round-robin start requests to each Shopper
    val shopperRouter = context.actorOf(Props(new Shopper(channel, Q)).withRouter(RoundRobinRouter(noOfShoppers)), name = "shopperRouter")
    // Count number of shopper's leaving the store.
    var noOfShoppersDied: Int = _

    def receive = {
      case SimulationStart =>
        log.info("Starting Simulation")
        for (i <- 0 until noOfShoppers) shopperRouter ! GoShop
      case ShopperDied =>
        noOfShoppersDied += 1
        if (noOfShoppersDied == noOfShoppers) {
          controller ! SimulationComplete
          context.stop(self)
        }
    }
  }

  /*
  * This is ugly. We pass in connection and channel because the Controller is best qualified to shutdown Rabbit client.
  * However, there's nothing to prevent them being shutdown elsewhere.
  * Infact, they are actively used elsewhere so it seems 'aneamic' to use them here!
   */
  class Controller(connection: com.rabbitmq.client.Connection, channel: com.rabbitmq.client.Channel) extends Actor with ActorLogging{
    def receive = {
      case SimulationComplete =>
        log.info("Simulation Finished.")
        channel.close()
        connection.close()
        // context is the current Actor and system returns the ActorSystem this Actor belongs to.
        context.system.shutdown()
    }
  }
  def generate(){
    val config = ConfigFactory.load()
    val noOfShoppers = config.getInt("basket_generator.noOfShoppers")
    Console.printf("Generator simulating %s shoppers.", noOfShoppers)

    // Set up RabbitMQ
    val connection = RabbitMQConnection.getConnection()
    val sendingChannel = connection.createChannel()
    sendingChannel.queueDeclare("GeneratedBasketQ", false, false, false, null)

    val actorSystem = ActorSystem("BasketGeneratorSystem", config)

    // used to control shutdown of the system when simulation finishes.
    val controller = actorSystem.actorOf(Props(new Controller(connection, sendingChannel)), name = "controller")
    // master controls the slaves/workers
    val shop = actorSystem.actorOf(Props(new Shop(noOfShoppers, sendingChannel, "GeneratedBasketQ", controller)), name = "master")

    shop ! SimulationStart
  }
}
