package app;

import java.io.IOException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.sql.Statement;

public class Receiver {
        private static final String QUEUE_NAME = "eventEstadoCuenta";
    
	public static void main(String[] args) throws Exception {
            String url = System.getenv().get("RABBITMQ_URL");

            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(url);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                  String message = new String(body, "UTF-8");
                  
                    try {
                        createMassiveProcess();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }                 
                }
            };
            channel.basicConsume(QUEUE_NAME, true, consumer);
	}
        
        public static void createMassiveProcess() throws Exception{
            try{ 
                
                String url = System.getenv().get("MYSQL_JDBC_URL_PATTERNS");
                String user = System.getenv().get("MYSQL_JDBC_USER_PATTERNS");
                String password = System.getenv().get("MYSQL_JDBC_PASSWORD_PATTERNS");
                Class.forName("com.mysql.jdbc.Driver");
                java.sql.Connection con = java.sql.DriverManager.getConnection(url,user,password);  

                Statement stmt=con.createStatement();  
                String sql = "INSERT INTO estado(code, process, balance, currency, fecha_venc, locked, customer_id) VALUES ";
                
                for(int indice = 3; indice <= 1000 ; indice++ ){                    
                    sql += " ('0000"+indice+"', 'NUEVO_ESTADO_CUENTA', "+random()+", 'PEN', curdate(), 0, 1),";
                }
                sql = sql.substring(0, sql.length()-1);

                stmt.execute(sql);
                con.close();  
            }catch(Exception e){ System.out.println(e);
            }  
        }
        
        public static int random(){
            return (((int) (Math.random() * 9) + 1)*10);
        }
}