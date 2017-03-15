package nablarch.fw.messaging.provider;

import nablarch.fw.messaging.InterSystemMessage.HeaderName;
import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.MessagingProvider;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.SendingMessage;
import nablarch.fw.messaging.provider.JmsMessagingProvider.Context.JmsHeaderName;
import nablarch.fw.messaging.provider.exception.BasicMessagingExceptionFactory;
import nablarch.test.core.messaging.EmbeddedMessagingProvider;
import org.junit.Test;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * JmsMessagingServerのテスト
 * 
 * @see JmsMessagingProvider
 * @author Iwauo Tajima
 */
public class JmsMessagingProviderTest {
    /** 次に受信するJMSメッセージ */
    private static Message receivingMessage;
    
    /** 送信したJMSメッセージ */
    private static Message sentMessage;
    
    /** エラーフラグ */
    private static boolean occursError = false;
    
    /**
     * MOMによるメッセージ再配信が行われた場合の挙動のテスト
     */
    @Test public void handlingRedeliveredMessages() throws Exception {
        receivingMessage = new StubMessage();
        receivingMessage.setJMSRedelivered(true);
        receivingMessage.setIntProperty("JMSXDeliveryCount", 5);
        
        MessagingProvider provider = createJmsProvider()
                                    .setRedeliveryLimit(5)
                                    .setPoisonQueueNamePattern("%s.DLQ");

        provider.createContext().receiveSync("LOCAL.RECEIVE");
        
        // 再配信上限をこえるとPoisonキューへの転送が発生し、
        // MessagingExceptionが送出される。
        receivingMessage.setIntProperty("JMSXDeliveryCount", 6);
        
        try {
            provider.createContext()
                    .receiveSync("LOCAL.RECEIVE");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof MessagingException);
            assertTrue(e.getMessage().contains("Expired redelivered limit."));
        }
        
        assertEquals(
            "LOCAL.RECEIVE.DLQ",
            ((Queue) sentMessage.getJMSDestination()).getQueueName()
        );
    }
    
    /**
     * JMSヘッダー/ユーザ定義属性の取り回しのテスト(受信時)
     */
    @Test public void readingHeadersFromAReceivedMessage() throws Exception {
        
        receivingMessage = new StubMessage();
        
        // JMS標準ヘッダ
        receivingMessage.setJMSDeliveryMode(1);
        receivingMessage.setJMSType("DUMMY");
        receivingMessage.setJMSPriority(9);
        
        // JMS拡張ヘッダ
        receivingMessage.setIntProperty("JMSXDeliveryCount", 3);
        
        // ユーザプロパティ
        receivingMessage.setStringProperty("NABLARCHProp1", "value1");
        
        ReceivedMessage received = createProvider()
                                  .createContext()
                                  .receiveSync("LOCAL.RECEIVE");
        
        Map<String, Object> headers = received.getHeaderMap();
        assertEquals(1,        headers.get(JmsHeaderName.DELIVERY_MODE));
        assertEquals("DUMMY",  headers.get(JmsHeaderName.TYPE));
        assertEquals(9,        headers.get(JmsHeaderName.PRIORITY));
        assertEquals(3,        headers.get("JMSXDeliveryCount"));
        assertEquals("value1", headers.get("NABLARCHProp1"));
    }
    
    /**
     * JMSヘッダー/ユーザ定義属性の取り回しのテスト(送信時)
     */
    @Test public void writingHeadersToASendingMessage() throws Exception {
        createProvider().createContext().send(new SendingMessage()
            .setHeader(JmsHeaderName.DELIVERY_MODE, 1)
            .setHeader(JmsHeaderName.TYPE,          "DUMMY")
            .setHeader(JmsHeaderName.PRIORITY,      9)
            .setHeader("JMSXDeliveryCount",         3)
            .setHeader("NABLARCHProp1",             "value1")
        );
        
        assertEquals(1,        sentMessage.getJMSDeliveryMode());
        assertEquals("DUMMY",  sentMessage.getJMSType());
        assertEquals(9,        sentMessage.getJMSPriority());
        assertEquals(3,        sentMessage.getIntProperty("JMSXDeliveryCount"));
        assertEquals("value1", sentMessage.getStringProperty("NABLARCHProp1"));
    }

    /**
     * メッセージ送受信の際にJエラーが発生した場合のテスト。
     */
    @Test public void handlingAnErrorWhileSendingOrReceivingAMessage()
    throws Exception {
        MessagingContext context = createProvider().createContext();
        
        // 宛先キュー名に対するDestinationオブジェクトが設定されていない場合は、
        // MessagingExceptionを送出する。
        occursError = false;
        try {
            context.send(
                new SendingMessage()
                   .setDestination("UNKNOWN.QUEUE")
            );
            fail();
            
        } catch (Exception e) {
            assertTrue(e instanceof MessagingException);
        }
        
        // 送信中にJMSExceptionが送出されるケース
        occursError = true;
        try {
            context.send(
                new SendingMessage()
                   .setDestination("SERVICE1.REQUEST")
            );
            fail();
            
        } catch (Exception e) {
            assertTrue(e instanceof MessagingException);
            assertTrue(e.getCause() instanceof JMSException);
            assertEquals("error", e.getCause().getMessage());
        }
        
        
        // 受信中にJMSExceptionが送出された場合。
        occursError = true;
        try {
            context.receiveSync("LOCAL.RECEIVE");
            fail();
            
        } catch (Exception e) {
            assertTrue(e instanceof MessagingException);
            assertTrue(e.getCause() instanceof JMSException);
            assertEquals("error", e.getCause().getMessage());
        }
        
        context.close(); // クローズ時にエラーが発生した場合はログ出力のみ。
        occursError = false;
    }

    
    /**
     * 接続エラーのテスト
     */
    @Test public void testGetContextWhileJmsProviderDoesNotWorkProperly()
    throws Exception {
        try {
            occursError = true;
            MessagingContext context = createProvider().createContext();
            fail();
            
        } catch (Exception e) {
            assertTrue(e instanceof MessagingException);
        } finally {
            occursError = false;
        }
    }
    
    @Test public void testGetConnectionFactory() throws Exception {
        JmsMessagingProvider provider = createJmsProvider();
        ConnectionFactory factory = provider.getConnectionFactory();
        assertThat(factory, is(instanceOf(StubConnectionFactory.class)));
    }

    /**
     * 退避キューの自動決定機能のテスト
     */
    @Test public void testDeterminingDeadLetterQueue() throws Exception {
        JmsMessagingProvider provider = createJmsProvider();
        provider.setDefaultPoisonQueue("DEFAULT.DLQ")
                .setPoisonQueueNamePattern("%s.DLQ");
        
        JmsMessagingProvider.Context
            context = (JmsMessagingProvider.Context) provider.createContext();
        
        Queue dlq = context.getPoisonQueueOf("LOCAL.RECEIVE");
        assertEquals("LOCAL.RECEIVE.DLQ", dlq.getQueueName());
        
        dlq = context.getPoisonQueueOf("SERVICE2.REQUEST");
        assertEquals("DEFAULT.DLQ", dlq.getQueueName());
    }

    private JmsMessagingProvider createJmsProvider() {
        return (JmsMessagingProvider) createProvider();
    }

    protected MessagingProvider createProvider() {
        JmsMessagingProvider provider = new JmsMessagingProvider()
              .setConnectionFactory(new StubConnectionFactory())
              .setDestinations(new HashMap<String, Queue>() {{
                  put("LOCAL.RECEIVE", new StubQueue("LOCAL.RECEIVE"));
                  put("LOCAL.RECEIVE.DLQ", new StubQueue("LOCAL.RECEIVE.DLQ"));
                  put("SERVICE1.REQUEST", new StubQueue("SERVICE1.REQUEST"));
                  put("SERVICE1.REQUEST.DLQ", new StubQueue("SERVICE1.REQUEST.DLQ"));
                  put("SERVICE2.REQUEST", new StubQueue("SERVICE2.REQUEST"));
                  put("DEFAULT.DLQ", new StubQueue("DEFAULT.DLQ"));
              }});
        provider.setMessagingExceptionFactory(new BasicMessagingExceptionFactory());
        return provider;
    }

    public static class StubQueue implements Queue {
        private final String name;
        public StubQueue(String name) {
            this.name = name;
        }
        public String getQueueName() throws JMSException {
            return name;
        }
    }
    
    public static class StubConnectionFactory implements ConnectionFactory {

        public Connection createConnection() throws JMSException {
            return new StubConnection();
        }

        public Connection createConnection(String arg0, String arg1) throws JMSException {
            return createConnection();
        }

    }

    public static class StubMessage implements BytesMessage {
        public void acknowledge() throws JMSException {

        }

        public void clearBody() throws JMSException {

        }

        private final Map<String, Object> properties = new HashMap<String, Object>();

        public void clearProperties() throws JMSException {
            properties.clear();
        }

        public boolean getBooleanProperty(String key) throws JMSException {
            return (Boolean) properties.get(key);
        }

        public byte getByteProperty(String key) throws JMSException {
            return (Byte) properties.get(key);
        }

        public double getDoubleProperty(String key) throws JMSException {
            return (Double) properties.get(key);
        }

        public float getFloatProperty(String key) throws JMSException {
            return (Float) properties.get(key);
        }

        public int getIntProperty(String key) throws JMSException {
            return (Integer) properties.get(key);
        }

        public String getJMSCorrelationID() throws JMSException {
            return correlationId;
        }
        private String correlationId;

        public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
            return correlationId.getBytes();
        }

        public int getJMSDeliveryMode() throws JMSException {
            return deliveryMode;
        }
        private int deliveryMode;

        public Destination getJMSDestination() throws JMSException {
            return destination;
        }
        private Destination destination;

        public long getJMSExpiration() throws JMSException {
            return expiration;
        }
        private long expiration;

        public String getJMSMessageID() throws JMSException {
            return messageId;
        }
        private String messageId = "dummyId";

        public int getJMSPriority() throws JMSException {
            return priority;
        }
        private int priority;

        public boolean getJMSRedelivered() throws JMSException {
            return redelivered;
        }
        private boolean redelivered;

        public Destination getJMSReplyTo() throws JMSException {
            return replyTo;
        }
        private Destination replyTo;

        public long getJMSTimestamp() throws JMSException {
            return timestamp;
        }
        private long timestamp;

        public String getJMSType() throws JMSException {
            return type;
        }
        private String type;

        public long getLongProperty(String key) throws JMSException {
            return (Long) properties.get(key);
        }

        public Object getObjectProperty(String key) throws JMSException {
            return properties.get(key);
        }

        public Enumeration<String> getPropertyNames() throws JMSException {
            final Iterator<String> names = properties.keySet().iterator();
            return new Enumeration<String>() {
                public boolean hasMoreElements() {
                    return names.hasNext();
                }
                public String nextElement() {
                    return names.next();
                }
            };
        }

        public short getShortProperty(String key) throws JMSException {
            return (Short) properties.get(key);
        }

        public String getStringProperty(String key) throws JMSException {
            return (String) properties.get(key);
        }

        public boolean propertyExists(String key) throws JMSException {
            return properties.containsKey(key);
        }

        public void setBooleanProperty(String key, boolean val)
        throws JMSException {
            properties.put(key, val);
        }

        public void setByteProperty(String key, byte val) throws JMSException {
            properties.put(key, val);
        }

        public void setDoubleProperty(String key, double val)
        throws JMSException {
            properties.put(key, val);
        }

        public void setFloatProperty(String key, float val)
        throws JMSException {
            properties.put(key, val);
        }

        public void setIntProperty(String key, int val) throws JMSException {
            properties.put(key, val);
        }

        public void setJMSCorrelationID(String header) throws JMSException {
            correlationId = header;
        }

        public void setJMSCorrelationIDAsBytes(byte[] header) throws JMSException {
            correlationId = new String(header);
        }

        public void setJMSDeliveryMode(int header) throws JMSException {
            deliveryMode = header;
        }

        public void setJMSDestination(Destination header) throws JMSException {
            destination = header;
        }

        public void setJMSExpiration(long header) throws JMSException {
            expiration = header;
        }

        public void setJMSMessageID(String header) throws JMSException {
            messageId = header;
        }

        public void setJMSPriority(int header) throws JMSException {
            priority = header;
        }

        public void setJMSRedelivered(boolean header) throws JMSException {
            redelivered = header;
        }

        public void setJMSReplyTo(Destination header) throws JMSException {
            replyTo = header;
        }

        public void setJMSTimestamp(long header) throws JMSException {
            timestamp = header;
        }

        public void setJMSType(String header) throws JMSException {
            type = header;
        }

        public void setLongProperty(String key, long val) throws JMSException {
            properties.put(key, (Long) val);
        }

        public void setObjectProperty(String key, Object val)
        throws JMSException {
            properties.put(key, val);
        }

        public void setShortProperty(String key, short val)
        throws JMSException {
            properties.put(key, val);
        }

        public void setStringProperty(String key, String val)
        throws JMSException {
            properties.put(key, (String) val);
        }

        public long getBodyLength() throws JMSException {
            return 0;
        }

        public boolean readBoolean() throws JMSException {
            return false;
        }

        public byte readByte() throws JMSException {
            return 0;
        }

        public int readBytes(byte[] arg0) throws JMSException {
            return 0;
        }

        public int readBytes(byte[] arg0, int arg1) throws JMSException {
            return 0;
        }

        public char readChar() throws JMSException {
            return 0;
        }

        public double readDouble() throws JMSException {
            return 0;
        }

        public float readFloat() throws JMSException {
            return 0;
        }

        public int readInt() throws JMSException {
            return 0;
        }

        public long readLong() throws JMSException {
            return 0;
        }

        public short readShort() throws JMSException {
            return 0;
        }

        public String readUTF() throws JMSException {
            return null;
        }

        public int readUnsignedByte() throws JMSException {
            return 0;
        }

        public int readUnsignedShort() throws JMSException {
            return 0;
        }

        public void reset() throws JMSException {

        }

        public void writeBoolean(boolean arg0) throws JMSException {

        }

        public void writeByte(byte arg0) throws JMSException {

        }

        public void writeBytes(byte[] arg0) throws JMSException {

        }

        public void writeBytes(byte[] arg0, int arg1, int arg2)
                throws JMSException {

        }

        public void writeChar(char arg0) throws JMSException {

        }

        public void writeDouble(double arg0) throws JMSException {

        }

        public void writeFloat(float arg0) throws JMSException {

        }

        public void writeInt(int arg0) throws JMSException {

        }

        public void writeLong(long arg0) throws JMSException {

        }

        public void writeObject(Object arg0) throws JMSException {

        }

        public void writeShort(short arg0) throws JMSException {

        }

        public void writeUTF(String arg0) throws JMSException {

        }
    }

    public static class StubProducer implements MessageProducer {
        public StubProducer(Queue queue) {
            this.queue = queue;
        }
        private Queue queue;
        public void close() throws JMSException {

        }

        public int getDeliveryMode() throws JMSException {
            return 0;
        }

        public Destination getDestination() throws JMSException {
            return null;
        }

        public boolean getDisableMessageID() throws JMSException {
            return false;
        }

        public boolean getDisableMessageTimestamp() throws JMSException {
            return false;
        }

        public int getPriority() throws JMSException {
            return 0;
        }

        public long getTimeToLive() throws JMSException {
            return 0;
        }

        public void send(Message message) throws JMSException {
            sentMessage = message;
            message.setJMSDestination(queue);
        }

        public void send(Destination arg0, Message message) throws JMSException {
            send(message);
        }

        public void send(Message message, int arg1, int arg2, long arg3)
        throws JMSException {
            send(message);
        }

        public void
        send(Destination arg0, Message message, int arg2, int arg3, long arg4)
        throws JMSException {
            send(message);
        }

        public void setDeliveryMode(int arg0) throws JMSException {

        }

        public void setDisableMessageID(boolean arg0) throws JMSException {

        }

        public void setDisableMessageTimestamp(boolean arg0)
                throws JMSException {

        }

        public void setPriority(int arg0) throws JMSException {

        }

        public void setTimeToLive(long arg0) throws JMSException {

        }
    }

    public static class StubConsumer implements MessageConsumer {

        public void close() throws JMSException {
        }

        public MessageListener getMessageListener() throws JMSException {
            return null;
        }

        public String getMessageSelector() throws JMSException {
            return null;
        }

        public Message receive() throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
            return receivingMessage;
        }

        public Message receive(long arg0) throws JMSException {
            return receive();
        }

        public Message receiveNoWait() throws JMSException {
            return receive();
        }

        public void setMessageListener(MessageListener arg0)
                throws JMSException {
        }
    }

    public static class StubSession implements Session {

        public void close() throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
        }

        public void commit() throws JMSException {

        }

        public QueueBrowser createBrowser(Queue arg0) throws JMSException {
            return null;
        }

        public QueueBrowser createBrowser(Queue arg0, String arg1)
                throws JMSException {
            return null;
        }

        public BytesMessage createBytesMessage() throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
            return new StubMessage();
        }

        public MessageConsumer createConsumer(Destination arg0)
        throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
            return new StubConsumer();
        }

        public MessageConsumer createConsumer(Destination arg0, String arg1)
        throws JMSException {
            return createConsumer(null);
        }

        public MessageConsumer createConsumer(Destination arg0,
                                              String      arg1,
                                              boolean     arg2)
        throws JMSException {
            return createConsumer(null);
        }

        public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1)
                throws JMSException {
            return null;
        }

        public TopicSubscriber createDurableSubscriber(Topic arg0, String arg1,
                String arg2, boolean arg3) throws JMSException {
            return null;
        }

        public MapMessage createMapMessage() throws JMSException {
            return null;
        }

        public Message createMessage() throws JMSException {
            return null;
        }

        public ObjectMessage createObjectMessage() throws JMSException {
            return null;
        }

        public ObjectMessage createObjectMessage(Serializable arg0)
                throws JMSException {
            return null;
        }

        public MessageProducer createProducer(Destination queue)
        throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
            return new StubProducer((Queue) queue);
        }

        public Queue createQueue(String arg0) throws JMSException {
            return null;
        }

        public StreamMessage createStreamMessage() throws JMSException {
            return null;
        }

        public TemporaryQueue createTemporaryQueue() throws JMSException {
            return null;
        }

        public TemporaryTopic createTemporaryTopic() throws JMSException {
            return null;
        }

        public TextMessage createTextMessage() throws JMSException {
            return null;
        }

        public TextMessage createTextMessage(String arg0) throws JMSException {
            return null;
        }

        public Topic createTopic(String arg0) throws JMSException {
            return null;
        }

        public int getAcknowledgeMode() throws JMSException {
            return 0;
        }

        public MessageListener getMessageListener() throws JMSException {
            return null;
        }

        public boolean getTransacted() throws JMSException {
            return false;
        }

        public void recover() throws JMSException {

        }

        public void rollback() throws JMSException {

        }

        public void run() {

        }

        public void setMessageListener(MessageListener arg0)
                throws JMSException {

        }

        public void unsubscribe(String arg0) throws JMSException {

        }
    }

    public static class StubConnection implements Connection {

        public void close() throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
        }

        public ConnectionConsumer
        createConnectionConsumer(Destination arg0,
                                 String arg1,
                                 ServerSessionPool arg2,
                                 int arg3) throws JMSException {
            return null;
        }

        public ConnectionConsumer createDurableConnectionConsumer(Topic arg0,
                String arg1, String arg2, ServerSessionPool arg3, int arg4)
                throws JMSException {
            return null;
        }

        public Session createSession(boolean arg0, int arg1) throws JMSException {
            if (occursError) {
                throw new JMSException("error");
            }
            return new StubSession();
        }

        public String getClientID() throws JMSException {
            return null;
        }

        public ExceptionListener getExceptionListener() throws JMSException {
            return null;
        }

        public ConnectionMetaData getMetaData() throws JMSException {
            return null;
        }

        public void setClientID(String arg0) throws JMSException {
            
        }

        public void setExceptionListener(ExceptionListener arg0)
                throws JMSException {
            
        }

        public void start() throws JMSException {
            
        }

        public void stop() throws JMSException {
            
        }
    }
    
    @Test public void meaningLess() throws Exception {
        Constructor<JmsHeaderName> constructor = JmsHeaderName.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        assertNotNull(constructor.newInstance());
        
        Constructor<HeaderName> constructor2 = HeaderName.class.getDeclaredConstructor();
        constructor2.setAccessible(true);
        assertNotNull(constructor2.newInstance());
    }
    
    @Test
    public void testSameReceivedQueueNameAndDifferentMessageIds() throws Exception {

        EmbeddedMessagingProvider provider = new EmbeddedMessagingProvider();
        EmbeddedMessagingProvider.waitUntilServerStarted();

        provider.setQueueNames(Arrays.asList("QUEUE"));
        MessagingContext context = provider.createContext();
        assertNotNull(context);

        context.send(new SendingMessage()
                             .setDestination("QUEUE")
                             .setMessageId("aaa")
                             .setCorrelationId("111"));

        context.send(new SendingMessage()
                             .setDestination("QUEUE")
                             .setMessageId("bbb")
                             .setCorrelationId("222"));

        context.send(new SendingMessage()
                             .setDestination("QUEUE")
                             .setMessageId("ccc")
                             .setCorrelationId("333"));

        ReceivedMessage message = context.receiveSync("QUEUE", "111", 100);
        assertThat(message.getMessageId(), is("aaa"));

        message = context.receiveSync("QUEUE", "222", 100);
        assertThat(message.getMessageId(), is("bbb"));

        message = context.receiveSync("QUEUE", "333", 100);
        assertThat(message.getMessageId(), is("ccc"));

        // サーバ終了
        context.close();
        EmbeddedMessagingProvider.stopServer();
    }
}
