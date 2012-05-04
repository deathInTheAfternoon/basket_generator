/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 02/05/12
 * Time: 10:26
 * 
 */

import akka.actor._
import akka.routing.RoundRobinRouter
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoCollection, MongoConnection, MongoDB}// MongoCollection is MT-safe so can be passed to Workers.
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
    private[this] var mongoConn: Option[MongoConnection] = None
    private[this] var db: Option[MongoDB] = None
    private[this] var coll: Option[MongoCollection] = None

    private[this] var conn: Option[com.rabbitmq.client.Connection] = None
    private[this] var chan: Option[com.rabbitmq.client.Channel] = None
    private[this] var noOfShoppers: Int = 0
    private[this] var queue: String  = "BGTestQ"

    // We round-robin start requests to each Shopper
    private var shopperRouter: Option[ActorRef] = None
    // Count number of shopper's leaving the store.
    var noOfShoppersDied: Int = _

    def receive = {
      case SimulationStart =>
        log.info("Start simulation of {} Shoppers. Pickup baskets at Q = {}", noOfShoppers, queue)
        for (i <- 0 until noOfShoppers) shopperRouter.get ! GoShop
      case ShopperDied =>
        noOfShoppersDied += 1
        if (noOfShoppersDied == noOfShoppers) {
          // Ask the 'user' generator to shutdown all its children (including Shop).
          context.system.shutdown()
        }
    }

    // Called before Actor starts accepting messages.
    override def preStart() = {
      val config = ConfigFactory.load()
      noOfShoppers = config.getInt("basketGenerator.noOfShoppers")
      queue = config.getString("basketGenerator.rabbitmq.queue")

      // Set up MongoDB
      mongoConn = Some(MongoConnection())//todo: defaulting to localhost:27017
      db = mongoConn map {c => c("BasketGenerator")}
      coll = db map {col => col("BasketCollection")}
      // todo: just a test
      val stamp = MongoDBObject("today" -> "18:51", "name" -> "Naveen")
      coll.get.save(stamp)
      log.info("Connected to MongoDB!")

      // Set up RabbitMQ
      conn = Some(RabbitMQConnection.getConnection())
      chan = conn map {c => c.createChannel()}
      chan.get.queueDeclare(queue, false, false, false, null)
      log.info("Connected to RabbitMQ!")
      shopperRouter = Some(context.actorOf(Props(new Shopper(chan.get, queue)).withRouter(RoundRobinRouter(noOfShoppers)), name = "shopperRouter"))
    }
    // Called when Actor terminates itself - having terminated its children.
    override def postStop() = {
      chan.get.close()
      conn.get.close()
    }
  }

  def generate(){
    val config = ConfigFactory.load()
    val actorSystem = ActorSystem("BasketGeneratorSystem", config)
    val shop = actorSystem.actorOf(Props[Shop], name = "topshop")

    shop ! SimulationStart
  }
}
