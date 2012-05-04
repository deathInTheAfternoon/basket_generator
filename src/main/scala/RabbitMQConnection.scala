import com.rabbitmq.client.{ConnectionFactory, Connection}

/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 02/05/12
 * Time: 17:15
 *
 */

object RabbitMQConnection {
  private val connection: Connection = null;

  def getConnection(): Connection = {
    connection match {
      case null => {
        val factory = new ConnectionFactory()
        factory.setHost("localhost")
        factory.newConnection()
      }
      case _ => connection
    }
  }
}
