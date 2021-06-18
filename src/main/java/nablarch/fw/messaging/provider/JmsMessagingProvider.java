package nablarch.fw.messaging.provider;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.annotation.Published;
import nablarch.fw.messaging.InterSystemMessage.HeaderName;
import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.MessagingProvider;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.SendingMessage;
import nablarch.fw.messaging.provider.exception.BasicMessagingExceptionFactory;

/**
 * JMSプロバイダを利用したメッセージング機能の実装。
 * 
 * 各JMSプロバイダが実装するConnectionFactoryおよびQueueオブジェクトを設定
 * することにより、メッセージング機能が利用可能となる。
 * <p/>
 * 
 * <dv><b>Poison電文の退避</b></div>
 * <hr/>
 * 本実装ではPoison電文の退避処理を独自に実装しており、リトライ上限、
 * 退避キュー名称を指定することができる。
 * ただし、この機能はJMSXDeliveryCountヘッダに依存しているため、同ヘッダを
 * サポートしない一部のMOM製品/バージョンでは利用できない。
 * なお、以下のMOMについては最新版における同ヘッダのサポートを確認している。
 * <pre>
 * - Websphere MQ
 * - WebLogic MQ
 * - ActiveMQ
 * </pre>
 * 
 * @author Iwauo Tajima
 */
public class JmsMessagingProvider implements MessagingProvider {

    /** メッセージングログを出力するロガー */
    private static final Logger LOGGER = LoggerManager.get("MESSAGING");

    // ------------------------------------------------------ structure
    /** JMSプロバイダによるコネクションファクトリ実装 */
    private ConnectionFactory factory;
    
    /** キュー名をキーとするJMS QueueオブジェクトのMap */
    private final Map<String, Queue> queueTable = new HashMap<String, Queue>();
    
    /** 退避キュー論理名のパターン */
    private String poisonQueueNamePattern = "%s.POISON";
    
    /** デフォルト退避キューの論理名 */
    private String defaultPoisonQueue = "DEFAULT.POISON";
    
    /** MOMによる受信リトライ上限値  */
    private int redeliveryLimit = 0; 
    
    /** 同期送信デフォルトタイムアウト値 (msec) */
    private long timeout = 300 * 1000L;
    
    /** 送信電文デフォルト有効期間 (msec) */
    private long timeToLive = 60 * 1000L;
    
    /** {@link MessagingException}ファクトリオブジェクト */
    private MessagingExceptionFactory messagingExceptionFactory = new BasicMessagingExceptionFactory();
    
    // ------------------------------------------------- MessagingProvider API
    /** {@inheritDoc}
     *  この実装では、コネクションファクトリからJMSコネクションを取得し、
     *  新規セッションを作成する。
     */
    public MessagingContext createContext() {
        try {
            Connection conn = factory.createConnection();
            conn.start();
            return new Context(conn, this);
            
        } catch (JMSException e) {
            throw messagingExceptionFactory.createMessagingException("failed to create context", e);
        }
    }
    
    /** {@inheritDoc}
     * 同期送信処理におけるデフォルトタイムアウト値を設定する。
     * デフォルトタイムアウトを明示的に設定しなかった場合のデフォルトタイムアウトは
     * 5分間となる。
     */
    public MessagingProvider setDefaultResponseTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    
    
    /** {@inheritDoc}
     * 送信電文のデフォルト有効期間を設定する。
     * デフォルト値を明示的に設定しなかった場合の有効期間は30秒(30000msec)となる。
     */
    public MessagingProvider setDefaultTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    // --------------------------------------------------------- accessors
    /**
     * コネクションファクトリを設定する。
     * 
     * このクラスではコネクションプール機能を提供していないため、
     * コネクションプール機能を内蔵したコネクションファクトリを使用することを
     * 強く推奨する。
     * 
     * @param factory コネクションファクトリ
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public JmsMessagingProvider setConnectionFactory(ConnectionFactory factory) {
        this.factory = factory;
        return this;
    }
    
    /**
     * コネクションファクトリを返す。
     * 
     * @return コネクションファクトリ
     */
    @Published(tag = "architect")
    public ConnectionFactory getConnectionFactory() {
        return factory;
    }
    
    /**
     * メッセージング機能で使用する宛先の論理名とJMS Destinationオブジェクトとの
     * マッピングを設定する。
     * （既存の設定があった場合は上書きされる。）
     * 
     * @param table キューの論理名とそれに対応するQueueオブジェクトとのマッピング
     * @return このオブジェクト自体
     */
    @Published(tag = "architect")
    public JmsMessagingProvider setDestinations(Map<String, Queue> table) {
        queueTable.clear();
        queueTable.putAll(table);
        return this;
    }
    
    /**
     * 各受信キューに対する退避キューの論理名を決定する際に使用する
     * パターン文字列を設定する。
     * 明示的に指定しなかった場合のデフォルトは、
     * <code>(受信キュー名).POISON</code>
     * となる。
     * 当該のキューが存在しなかった場合はデフォルトの退避キュー名を使用する。
     * 
     * @param pattern 退避キューの論理名を決定する際に使用するパターン文字列
     * @return このオブジェクト自体
     */
    public JmsMessagingProvider setPoisonQueueNamePattern(String pattern) {
        poisonQueueNamePattern = pattern;
        return this;
    }
    
    /**
     * デフォルトで使用する受信退避キューの論理名を設定する。
     * 当該のキューが存在しなかった場合は、MessgingExceptionを送出する。
     * 明示的に指定しなかった場合は、<code>DEFAULT.POISON</code>を使用する。
     * @param queueName キュー名称
     * @return このオブジェクト自体
     */
    public JmsMessagingProvider setDefaultPoisonQueue(String queueName) {
        defaultPoisonQueue = queueName;
        return this;
    }
    
    /**
     * MOMによる受信リトライの上限回数を設定する。
     * 受信メッセージのJMSXDeliveryCountヘッダの値がこの上限値を越えると、
     * メッセージを退避キューに転送した上で、MessagingExceptionを送出する。
     * 
     * この値が0以下の数値であった場合は、退避処理自体が無効化される。
     * 明示的に指定しない場合のデフォルトは0である。
     * 
     * @param limit 受信リトライの上限回数
     * @return このオブジェクト自体。
     */
    public JmsMessagingProvider setRedeliveryLimit(int limit) {
        redeliveryLimit = limit;
        return this;
    }
    
    /**
     * {@link MessagingException}ファクトリオブジェクトを設定する。
     * <p/>
     * デフォルトは{@link BasicMessagingExceptionFactory}。
     * 
     * @param messagingExceptionFactory {@link MessagingException}ファクトリオブジェクト
     * @return このオブジェクト自体
     */
    public MessagingProvider setMessagingExceptionFactory(
            MessagingExceptionFactory messagingExceptionFactory) {
        this.messagingExceptionFactory = messagingExceptionFactory;
        return this;
    }
    
    /**
     *  メッセージングコンテキストのJMSベース実装
     */
    public static class Context extends MessagingContext {
        // ---------------------------------------------------- Structure
        /** このインスタンスが使用するJMSプロバイダに対するコネクション */
        private final Connection conn;
        
        /** このインスタンスが使用するJMSセッション */
        private final Session sess;
        
        /** 各種設定 */
        private final JmsMessagingProvider provider;

        /** MessageProducerを保持しておくキャッシュ */
        private Map<String, MessageProducer> producerCache = new HashMap<String, MessageProducer>();

        /** MessageConsumerを保持しておくキャッシュ */
        private Map<String, MessageConsumer> consumerCache = new HashMap<String, MessageConsumer>();

        // ---------------------------------------------------- Constructor
        /**
         * コンストラクタ
         * 
         * @param conn JMSセッション
         * @param provider 各種設定
         * @throws JMSException JMSプロバイダ側でエラーが発生した場合
         */
        public Context(Connection conn, JmsMessagingProvider provider)
        throws JMSException {
            this.conn     = conn;
            this.sess     = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.provider = provider;
        }
        
        // ---------------------------------------------- MessagingContext API
        /** {@inheritDoc}
         * 使用中のセッションを終了し、コネクションをプールに返却する。
         * 
         * この実装では、JMSコネクションファクトリがコネクションプールを内蔵する
         * ことを前提としており、コネクションを単にclose()している。
         * 
         * キャッシュしているMessageProducerとMessageConsumerをクローズする。
         * MessageProducerとMessageConsumerのクローズ処理で例外が発生した場合、
         * TRACEレベルのログ出力を行い、例外の再スローは行わない。
         */
        public void close() {
            for (MessageProducer producer : producerCache.values()) {
                try {
                    producer.close(); // 念のため。
                } catch (JMSException e) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.logTrace("could not close JMS Producer.", e);
                    }
                }
            }
            for (MessageConsumer consumer : consumerCache.values()) {
                try {
                    consumer.close(); // 念のため。
                } catch (JMSException e) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.logTrace("could not close JMS Consumer.", e);
                    }
                }
            }
            try {
                sess.close(); // 念のため。
            } catch (JMSException e) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.logTrace("could not close JMS Session.", e);
                }
            }
            try {
                conn.close(); 
            } catch (JMSException e) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.logTrace("could not close JMS Connection.", e);
                }
            }
        }

        /**
         * 宛先キューの名前に応じたMessageProducerを生成する。
         * <p/>
         * 宛先キューの名前毎に生成したMessageProducerをキャッシュする。
         * このため、宛先キューの論理名に対応するMessageProducerがキャッシュに存在する場合、
         * MessageProducerを生成せずに、キャッシュに存在するMessageProducerを返す。
         * <p/>
         * 送信電文デフォルト有効期間({@link JmsMessagingProvider#timeToLive})を生成したMessageProducerに設定する。
         * 
         * @param queueName 宛先キューの名前
         * @return 宛先キューの名前に応じたMessageProducer
         * @throws JMSException JMSプロバイダ側でエラーが発生した場合
         */
        private MessageProducer createProducer(String queueName) throws JMSException {
            MessageProducer producer = producerCache.get(queueName);
            if (producer == null) {
                producer = sess.createProducer(getQueueOf(queueName));
                producer.setTimeToLive(provider.timeToLive);
                producerCache.put(queueName, producer);
            }
            return producer;
        }

        /** {@inheritDoc}
         *  この実装では、JMSの実装系を使用して送信処理を行う。
         */
        public String sendMessage(SendingMessage message) {
            try {
                BytesMessage jmsMessage = sess.createBytesMessage();
                writeHeadersTo(jmsMessage, message.getHeaderMap());
                jmsMessage.writeBytes(message.getBodyBytes());
                MessageProducer producer = createProducer(message.getDestination());
                producer.send(jmsMessage);
                
                String messageId = jmsMessage.getJMSMessageID();
                message.setMessageId(messageId);
                return messageId;
                
            } catch (JMSException e) {
                throw provider.messagingExceptionFactory.createMessagingException(
                        "an error occurred while sending the message.", e);
            }
        }

        /**
         * 受信キューの名前に応じたMessageConsumerを生成する。
         * <p/>
         * メッセージIDを指定した場合、指定されたメッセージIDを関連IDに指定したセレクタを用いて
         * MessageConsumerを生成する。
         * セレクタを指定したMessageConsumerは再利用できないため、本メソッドが呼ばれるたびに生成する。
         * 
         * メッセージIDを指定しなかった場合、セレクタを指定せずにMessageConsumerを生成する。
         * セレクタを指定しないMessageConsumerは再利用可能なため、
         * 受信キューの名前毎に生成したMessageConsumerをキャッシュする。
         * 受信キューの論理名に対応するMessageConsumerがキャッシュに存在する場合、
         * MessageConsumerを生成せずに、キャッシュに存在するMessageConsumerを返す。
         * 
         * @param queueName 受信キューの名前
         * @param messageId MessageConsumerの生成時に指定するセレクタに使用するメッセージID。
         *                   セレクタを指定しない場合はnull
         * @return 受信キューの名前に応じたMessageConsumer
         * @throws JMSException JMSプロバイダ側でエラーが発生した場合
         */
        private MessageConsumer createConsumer(String queueName, String messageId) throws JMSException {
            if (messageId != null) {
                String selector = "JMSCorrelationID = '" + messageId + "'";
                Queue queue = getQueueOf(queueName);
                return sess.createConsumer(queue, selector);
            } else {
                MessageConsumer consumer = consumerCache.get(queueName);
                if (consumer == null) {
                    Queue queue = getQueueOf(queueName);
                    consumer = sess.createConsumer(queue);
                    consumerCache.put(queueName, consumer);
                }
                return consumer;
            }
        }

        /** {@inheritDoc}
         *  この実装では、JMSの実装系を使用して指定されたキュー上のメッセージの
         *  同期受信を行う。
         */
        public ReceivedMessage
        receiveMessage(String queueName, String messageId, long timeout) {
            MessageConsumer consumer = null;
            try {
                timeout = (timeout <= 0) ? provider.timeout
                                         : timeout;
                consumer = createConsumer(queueName, messageId);
                Message received = consumer.receive(timeout);
                if (received == null) {
                    return null;
                }
                if (received.getJMSRedelivered()) {
                    rejectIfExpiresRedeliverLimit(received, queueName);
                }
                ReceivedMessage message;
                // メッセージボディ
                if (received instanceof BytesMessage) {
                    BytesMessage bm = (BytesMessage) received;
                    byte[] unparsedData = new byte[(int) bm.getBodyLength()];
                    bm.readBytes(unparsedData);
                    message = new ReceivedMessage(unparsedData);
                    
                } else {
                    LOGGER.logWarn(
                      "Could not parse the body of the received message, "
                    + "because the type of it was not 'BytesMessage'."
                    );
                    message = new ReceivedMessage(new byte[0]);
                }
                // メッセージヘッダー
                Map<String, Object> headers = message.getHeaderMap();
                readHeadersFrom(received, headers);
                return message;
                
            } catch (JMSException e) {
                throw provider.messagingExceptionFactory.createMessagingException(
                                    "an error occurred while receiving a message.", e);
            } finally {
                try {
                    if (consumer != null && !consumerCache.containsValue(consumer)) {
                        consumer.close();
                    }
                } catch (JMSException e) {
                    LOGGER.logWarn("could not close JMS Consumer.", e);
                }
            }
        }
        
        // ------------------------------------------------------ helper methods
        /**
         * JMSメッセージヘッダを読み込む。
         * @param message JMSメッセージ
         * @param headers ヘッダーを格納するマップ
         * @throws JMSException JMS API側の内部で問題が発生した場合。
         */
        @SuppressWarnings("unchecked")
        private void readHeadersFrom(Message message, Map<String, Object> headers) 
        throws JMSException {
            /*
             * 規定JMSヘッダー
             */
            headers.put(JmsHeaderName.MESSAGE_ID, message.getJMSMessageID());
            headers.put(JmsHeaderName.CORRELATION_ID, message.getJMSCorrelationID());
            headers.put(JmsHeaderName.DESTINATION, getQueueNameOf(message.getJMSDestination()));
            headers.put(JmsHeaderName.REPLY_TO, getQueueNameOf(message.getJMSReplyTo()));
            /*
             * JMS標準ヘッダ
             */
            headers.put(JmsHeaderName.DELIVERY_MODE, message.getJMSDeliveryMode());
            headers.put(JmsHeaderName.TYPE, message.getJMSType());
            headers.put(JmsHeaderName.PRIORITY, message.getJMSPriority());
            headers.put(JmsHeaderName.TIMESTAMP, message.getJMSTimestamp());
            headers.put(JmsHeaderName.EXPIRATION, message.getJMSExpiration());
            headers.put(JmsHeaderName.REDELIVERED, message.getJMSRedelivered());
            /*
             * ユーザ定義属性およびJMS拡張ヘッダ
             */
            Enumeration<String> propNames = message.getPropertyNames();
            while (propNames.hasMoreElements()) {
                String headerName = propNames.nextElement();
                headers.put(headerName, message.getObjectProperty(headerName));
            }
        }
        
        /**
         * JMSメッセージヘッダを設定する。
         * @param message JMSメッセージオブジェクト
         * @param headers 設定するヘッダの内容
         * @throws JMSException JMS APIの内部で問題が発生した場合。
         */
        private void writeHeadersTo(Message message, Map<String, Object> headers)
        throws JMSException {
            /*
             * 既定ヘッダ
             * JMSDestination/JMSMessageIDについてはMOM側で自動設定されるため、
             * ここでは設定しない。
             */
            if (headers.containsKey(JmsHeaderName.CORRELATION_ID)) {
                message.setJMSCorrelationID(
                    (String) headers.get(JmsHeaderName.CORRELATION_ID)
                );
            }
            if (headers.containsKey(JmsHeaderName.REPLY_TO)) {
                message.setJMSReplyTo(
                    getQueueOf((String) headers.get(JmsHeaderName.REPLY_TO))
                );
            }
            /*
             * JMSヘッダ
             * JMSTimestamp/JMSExpiration/JMSRedeliveredについては
             * MOM側で自動設定されるため、ここでは設定しない。
             */
            if (headers.containsKey(JmsHeaderName.DELIVERY_MODE)) {
                message.setJMSDeliveryMode(
                    (Integer) headers.get(JmsHeaderName.DELIVERY_MODE)
                );
            }
            if (headers.containsKey(JmsHeaderName.TYPE)) {
                message.setJMSType(
                    (String) headers.get(JmsHeaderName.TYPE)
                );
            }
            if (headers.containsKey(JmsHeaderName.PRIORITY)) {
                message.setJMSPriority(
                    (Integer) headers.get(JmsHeaderName.PRIORITY)
                );
            }
            /*
             * ユーザ定義属性 / JMS拡張ヘッダ
             */
            for (Entry<String, Object> header : headers.entrySet()) {
                String name  = header.getKey();
                Object value = header.getValue();
                if (name.startsWith("JMSX")) {          // JMS拡張ヘッダ
                    message.setObjectProperty(name, value);
                    continue;
                }
                if (name.startsWith("JMS")) {           // JMSヘッダ
                    continue;
                }
                message.setObjectProperty(name, value); // ユーザ定義属性
            }
        }
        
        /**
         * JMSヘッダー名称
         */
        public static final class JmsHeaderName {
            /*
             * 既定ヘッダ (プロバイダの外部で直接使用)
             */
            /** 送信先キュー (jakarta.jms.Destination:送信側で自動設定) */
            public static final String DESTINATION    = HeaderName.DESTINATION;
            /** 応答先キュー (jakarta.jms.Destination:null) */
            public static final String REPLY_TO       = HeaderName.REPLY_TO;
            /** メッセージID (String) */
            public static final String MESSAGE_ID     = HeaderName.MESSAGE_ID;
            /** 関連メッセージID (String) */
            public static final String CORRELATION_ID = HeaderName.CORRELATION_ID;
            /*
             * JMSヘッダ (プロバイダの内部のみで使用)
             */
            /** メッセージパーシステンス設定 (int デフォルト:DeliveryMode.PERSISTENT) */
            public static final String DELIVERY_MODE = "JMSDeliveryMode";
            /** メッセージタイプ (String　デフォルト:"") */
            public static final String TYPE          = "JMSTYPE";
            /** メッセージの優先度 (int (0-9) デフォルト:4)*/
            public static final String PRIORITY      = "JMSPriority";
            /** 送信日時(msec) (long 送信側で自動設定) */
            public static final String TIMESTAMP     = "JMSTimestamp";
            /** メッセージ有効期限日時 (long 送信側で自動設定) */
            public static final String EXPIRATION    = "JMSExpiration";
            /** 再取得の有無 (boolean デフォルト:false) */
            public static final String REDELIVERED   = "JMSRedelivered";
            /*
             * JMS拡張ヘッダ
             * (MOMによってサポート状況が異なるので極力使用しない。)
             */
            /** 再取得処理の回数 (int デフォルト:0) */
            public static final String X_DELIVERY_COUNT = "JMSXDeliveryCount";
            /** 電文グループID (String デフォルト:null) */
            public static final String X_GROUP_ID       = "JMSXGroupId";
            /** 電文のグループ内通番 (int デフォルト:0) */
            public static final String X_GROUP_ID_SEQ   = "JMSXGroupIdSeq";
            /** 送信側のトランザクションID (String デフォルト:null) */
            public static final String X_PRODUCER_TXID  = "JMSXProducerTXID";
            
            /** 定数クラスなのでインスタンスは不要 */
            private JmsHeaderName() {
            }
        }

        /**
         * 指定されたキュー名に対するJMSQueueインスタンスを返す。
         * 
         * @param queueName 取得するキューの論理名
         * @return JMSQueueインスタンス
         * @throws MessagingException
         *              指定されたキュー名に対するキューオブジェクトが登録されて
         *              いなかった場合。
         */
        protected Queue getQueueOf(String queueName) throws MessagingException {
            if (queueName == null) {
                return null;
            }
            Queue queue = provider.queueTable.get(queueName);
            if (queue == null) {
                throw new MessagingException("unknown queue name: " + queueName);
            }
            return queue;
        }
        
        /**
         * 指定されたJMSキューの論理名を逆引きする。
         * キューが登録されていない場合はnullを返す。
         * @param queue キューインスタンス
         * @return キューの論理名
         */
        protected String getQueueNameOf(Destination queue) {
            for (Map.Entry<String, Queue> entry : provider.queueTable.entrySet()) {
                if (entry.getValue().equals(queue)) {
                    return entry.getKey();
                }
            }
            return null;
        }
        
        /**
         * 指定されたJMSキューに対する退避キューを返す。
         * 該当するキューが存在しない場合はMessagingExceptionを送出する。
         * @param queueName 受信キューの論理名
         * @return JMSQueueインスタンス
         * @throws MessagingException
         *          退避キューが定義されていない場合。
         */
        protected Queue getPoisonQueueOf(String queueName)
        throws MessagingException {
            String poisonQueueName = String.format(
                provider.poisonQueueNamePattern, queueName
            );
            if (provider.queueTable.containsKey(poisonQueueName)) {
                return getQueueOf(poisonQueueName);
            }
            if (provider.queueTable.containsKey(provider.defaultPoisonQueue)) {
                return getQueueOf(provider.defaultPoisonQueue);
            }
            throw new MessagingException(
                "There were not any poison queue for the queue of " + queueName
            );
        }
        
        /**
         * MOMによるメッセージの受信リトライ回数が規定回数を越えていた場合は、
         * メッセージ退避キューに転送し、実行時例外を送出する。
         * 
         * @param message   受信メッセージ
         * @param queueName 受信キューの論理名
         * @throws MessagingException 受信リトライ回数が規定回数を越えていた場合
         * @throws JMSException JMS API側で問題が発生した場合。
         */
        protected void rejectIfExpiresRedeliverLimit(Message message, String queueName)
        throws MessagingException, JMSException {
            if (provider.redeliveryLimit <= 0) {
                return;
            }
            int redeliveryCount = message.getIntProperty(JmsHeaderName.X_DELIVERY_COUNT);
            if (redeliveryCount <= provider.redeliveryLimit) {
                return;
            }
            Queue poisonQueue = getPoisonQueueOf(queueName);
            message.setJMSCorrelationID(message.getJMSMessageID());
            sess.createProducer(poisonQueue).send(message);
            throw new MessagingException("Expired redelivered limit.");
        }
    }
}
