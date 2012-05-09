/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 02/05/12
 * Time: 10:26
 * 
 */

import akka.actor._
import akka.routing.RoundRobinRouter
import collection.mutable.HashMap
import com.mongodb.casbah.Imports._
import com.rabbitmq.client.{AMQP, Channel}
import java.util.Date
import scala.None
import scala.Predef._
import scala.util.parsing.json._

// MongoCollection is MT-safe so can be passed to Workers.
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
  case class GoShop(shopperId: String) extends SimulatorMessage
  case object ShopperDied extends SimulatorMessage

  /** A ghost which occupies the store. It is given an Id by the Master when told to Shop. It publishes receipts on
   * the Q.
   *
   * The current problem is that a single Channel serialzes requests (1 thd at a time is able to run commands).
   * This will become a bottleneck if many Actors are created. The alternative (as suggested) is run a Channel per-thd.
   * Does it make sense to implement this as a Channel per Event-Based Actor?
   * What happens if there are many Actors, in this case?
   * Yet another way might be to have a tuneable pool of Channels? But then how do we ensure two EBA's on the same thd
   * don't get the same Channel and start serialising again?
   * Hopefully, Akka 2.1 will bring full Camel migration which will be the recommended way to Q.
   *
   * @param channel is the RabbitMQ Channel object
   * @param Q is the name of the Rabbit queue.
   * @param referenceDataCollection the MongoDB Collection which stores reference-data.
   *
   */
  class Shopper(channel: Option[Channel], Q: String, referenceDataCollection: Option[MongoCollection],
                 noOfSKUs: Int, logsCollection: Option[MongoCollection]) extends Actor with ActorLogging{   //todo: is there a better way to pass noOfSKus and other constants?
    def receive = {
      case GoShop(shopperId) =>
        val basket = generateBasket(shopperId)
        logsCollection.get.save(basket)// log the basket we're about to Q.

        // We call .toMap to convert from Mutable to Immutable map.
        val builder = new AMQP.BasicProperties.Builder
        // It's important to set the contentType property - otherwise node-amqp will see bytes instead of JSON.
        channel.get.basicPublish("", Q, builder.contentType("application/json").build(), JSONObject(basket).toString().getBytes);

        log.info(JSONObject(basket).toString())
        // terminate this shopper's stint
        sender ! ShopperDied
    }

    /** Generates a randomised basket of shopping.
     * This method generates a basket of shopping with a random number of items and a random selection of products. It
     * assumes the data source contains SKUs with a seqId: Int. The seqId is used to lookup a SKU given a random number.
     *
     * @param forShopper is the id for the person who owns the basket.
     * @return A map of skuId->descriptions as well as id->forShopper - in no particular order.
     */
    def generateBasket(forShopper: String): Map[String, AnyRef] = {
      // Building query: { sku: {$in: [sku1,...skuN } }, where N is random number
      // generating random N between 1 and 5.
      val N = util.Random.nextInt(5)
      // generating list of N skus.
      var skuList = List.empty[Any]
      for (i <- 0 to N) {
        skuList ++= List(util.Random.nextInt(noOfSKUs))
      }

      val query: MongoDBObject = "seqId" $in (skuList) //note, each basket will contain a sku once only.

      // Fire the query to get back the full item details from the ref data, for the receipt.
      var itemDetails = new HashMap[String, AnyRef]
      var itemLine = 0
      for ( x <- referenceDataCollection.get.find(query))
      {
        itemDetails += ("item" + itemLine) -> x.get("NAME")
        itemLine += 1
      }
      // Add meta-data
      itemDetails += ("id" -> forShopper)
      itemDetails += ("date" -> new Date().toString)
      itemDetails.toMap
    }
  }

  /** Represents the Shop in which Shoppers will soon arrive and start shopping.
   *
   * SimulationStart event - begins Shopper activity, is sent from Controller.
   * ShopperDied event - sent from Shopper to indicate they're leaving the store.
   *
   **/

  class Shop extends Actor with ActorLogging {
    private[this] var mongoConn: Option[MongoConnection] = None
    private[this] var db: Option[MongoDB] = None
    private[this] var referenceDataCollection: Option[MongoCollection] = None
    private[this] var logsCollection: Option[MongoCollection] = None

    private[this] var conn: Option[com.rabbitmq.client.Connection] = None
    private[this] var chan: Option[com.rabbitmq.client.Channel] = None
    private[this] var noOfAkkaActors: Int = _
    private[this] var noOfShoppers: Int = _
    private[this] var queue: String  = "BGTestQ"

    // We round-robin start requests to each Shopper
    private var shopperRouter: Option[ActorRef] = None
    // Count number of shopper's leaving the store.
    var noOfShoppersDied: Int = _
    var noOfSKUs: Int = _ // range for random numbers used to generate skus.

    def receive = {
      case SimulationStart =>
        log.info("Start simulation of {} Shoppers using {} AkkaActors. Pickup baskets at Q = {}", noOfShoppers, noOfAkkaActors, queue)
        for (i <- 0 until noOfShoppers) shopperRouter.get ! GoShop(nextASCIIString(20))  // todo: generate shopper id from database
      case ShopperDied =>
        noOfShoppersDied += 1
        if (noOfShoppersDied == noOfShoppers) {
          log.info("Finished simulation of {} Shoppers!", noOfShoppers)
          // Ask the 'user' generator to shutdown all its children (including Shop).
          context.system.shutdown()
        }
    }

    def nextASCIIString(length: Int) = {
      val (min, max) = (33, 126)
      def nextDigit = util.Random.nextInt(max - min) + min

      new String(Array.fill(length)(nextDigit.toByte), "ASCII")
    }

    // Called before Actor starts accepting messages.
    override def preStart() {
      noOfShoppers = Config.GENERATOR_NO_OF_SHOPPERS
      noOfAkkaActors = Config.GENERATOR_NO_OF_AKKA_ACTORS
      queue = Config.RABBITMQ_QNAME

      // Set up MongoDB
      mongoConn = Some(MongoConnection(Config.MONGODB_HOST, Config.MONGODB_PORT))
      db = mongoConn map {c => c(Config.MONGODB_DATABASE)}
      // MongoCollection is MT-safe
      // (See: https://github.com/typesafehub/webwords/blob/heroku-devcenter/common/src/main/scala/com/typesafe/webwords/common/IndexStorageActor.scala).
      referenceDataCollection = db map {col => col(Config.GENERATOR_REF_DATA_COLLECTION)}
      logsCollection = db map {col => col(Config.GENERATOR_LOGS_COLLECTION)}
      log.info("Connected to MongoDB!")
      // log start of activity
      val stamp = MongoDBObject("date" -> new Date().toString, "event" -> "Test Run!")
      logsCollection.get.save(stamp)
      // capture total number of SKUs.
      noOfSKUs = referenceDataCollection.get.find(MongoDBObject("domainRefType" -> "SKU")).count

      // Set up RabbitMQ
      conn = Some(RabbitMQConnection.getConnection())
      chan = conn map {c => c.createChannel()}
      chan.get.queueDeclare(queue, false, false, false, null)
      log.info("Connected to RabbitMQ!")
      shopperRouter = Some(context.actorOf(Props(new Shopper(chan, queue, referenceDataCollection, noOfSKUs, logsCollection)).withRouter(RoundRobinRouter(noOfAkkaActors)), name = "shopperRouter"))
    }
    // Called when Actor terminates, having terminated its children.
    override def postStop() {
      // 'foreach' applies the function to the Options value if its non-empty (!= None).

      //Rabbit
      chan foreach (ch => ch.close()) //Good practice! But not necessary if the connection is being closed.
      conn foreach (co => co.close())
      //Mongo
      mongoConn foreach(c => c.close())
      // Let's be really neat!
      chan = None
      conn = None
      mongoConn = None
    }
  }

  def generate(){
    val config = ConfigFactory.load()
    val actorSystem = ActorSystem("BasketGeneratorSystem", config)
    val shop = actorSystem.actorOf(Props[Shop], name = "topshop")

    shop ! SimulationStart
  }

  /**
   * Nice way to encapsulate access to configuration data.
   */
  object Config {
    val MONGODB_HOST = ConfigFactory.load().getString("basketGenerator.mongodb.host")
    val MONGODB_PORT = ConfigFactory.load().getInt("basketGenerator.mongodb.port")
    val MONGODB_DATABASE = ConfigFactory.load().getString("basketGenerator.mongodb.database")
    val RABBITMQ_HOST = ConfigFactory.load().getString("basketGenerator.rabbitmq.queue")
    val RABBITMQ_QNAME = ConfigFactory.load().getString("basketGenerator.rabbitmq.queue")
    val GENERATOR_NO_OF_AKKA_ACTORS = ConfigFactory.load().getInt("basketGenerator.noOfAkkaActors")
    val GENERATOR_NO_OF_SHOPPERS = ConfigFactory.load().getInt("basketGenerator.noOfShoppers")
    val GENERATOR_REF_DATA_COLLECTION = ConfigFactory.load().getString("basketGenerator.mongodb.referenceDataCollection")
    val GENERATOR_LOGS_COLLECTION = ConfigFactory.load().getString("basketGenerator.mongodb.logsCollection")
  }
}
