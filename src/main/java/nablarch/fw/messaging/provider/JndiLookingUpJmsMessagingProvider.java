package nablarch.fw.messaging.provider;

import nablarch.core.repository.jndi.JndiHelper;
import nablarch.core.util.StringUtil;
import nablarch.fw.messaging.MessagingContext;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link ConnectionFactory}および{@link Queue}をJNDIから取得する
 * {@link nablarch.fw.messaging.MessagingProvider}実装クラス。<br/>
 * リソースの取得をJNDIから行う以外は、{@link JmsMessagingProvider}と同様。
 * 設定例をいかに示す。
 * <pre>
 * {@literal
 * <!-- メッセージングプロバイダ定義 -->
 * <component name="messagingProvider"
 *          class="nablarch.fw.messaging.provider.JndiLookingUpJmsMessagingProvider">
 *   <!-- 本クラス独自のプロパティ（JNDI関連）-->
 *   <!-- JNDIルックアップのための情報 -->
 *   <property name="jndiHelper">
 *     <component class="nablarch.core.repository.jndi.JndiHelper">
 *       <property name="jndiProperties">
 *         <map>
 *            <entry key="java.naming.factory.initial" value="weblogic.jndi.WLInitialContextFactory"/>
 *            <entry key="java.naming.provider.url"    value="t3://weblogic.server.125:7001"/>
 *         </map>
 *       </property>
 *     </component>
 *   </property>
 *   <!-- javax.jms.ConnectionFactory実装クラスをルックアップするためのJNDI名 -->
 *   <property name="connectionFactoryJndiName" value="javax.jms.QueueConnectionFactory"/>
 *
 *   <!-- FWで使用するキュー論理名と、java.jms.Queue実装クラスをルックアップするためのJNDI名のペア -->
 *   <property name="destinationNamePairs">
 *     <map>
 *       <!-- key=キュー論理名、value=キューJNDI名 -->
 *       <entry key="TEST.REQUEST"  value="TEST.REQUEST"/>
 *       <entry key="TEST.RESPONSE" value="TEST.RESPONSE"/>
 *     </map>
 *   </property>
 *   <!-- その他のプロパティは親クラス(JmsMessagingProvider)と同じ -->
 * </component>
 * }
 * </pre>
 *
 * @author T.Kawasaki
 * @see JndiHelper
 */
public class JndiLookingUpJmsMessagingProvider extends JmsMessagingProvider {

    /** JNDIヘルパークラス */
    private JndiHelper jndiHelper;

    /** {@link ConnectionFactory}のJNDI名 */
    private String connectionFactoryJndiName;

    /**
     * key=キュー論理名、value=キューJNDI名をペアにしたMap。
     *
     * @see #lookUpQueues(java.util.Map)
     */
    private Map<String, String> destinationNamePairs;

    /** {@inheritDoc} */
    public MessagingContext createContext() {
        prepareIfNecessary();
        return super.createContext();
    }


    /**
     * JNDIサポートクラスを設定する。
     *
     * @param jndiHelper JNDIサポートクラス
     * @return 本インスタンス
     */
    public JndiLookingUpJmsMessagingProvider setJndiHelper(JndiHelper jndiHelper) {
        this.jndiHelper = jndiHelper;
        return this;
    }

    /**
     * key=キュー論理名、value=キューJNDI名をペアにしたMapを設定する。<br/>
     * このMapは、{@link Queue}をJNDIから取得する際に使用される。
     * value(キューJNDI名)から{@link Queue}がルックアップされ、
     * key(キュー論理名）と紐付けられて、{@link JmsMessagingProvider}に設定される。
     *
     * @param pairs key=キュー論理名、value=キューJNDI名をペアにしたMap
     * @return 本インスタンス
     */
    public JndiLookingUpJmsMessagingProvider setDestinationNamePairs(Map<String, String> pairs) {
        this.destinationNamePairs = pairs;
        return this;
    }

    /**
     * {@link ConnectionFactory}をJNDIから取得するためのJNDI名を設定する。
     *
     * @param connectionFactoryJndiName {@link ConnectionFactory}のJNDI名
     * @return 本インスタンス
     */
    public JndiLookingUpJmsMessagingProvider setConnectionFactoryJndiName(String connectionFactoryJndiName) {
        this.connectionFactoryJndiName = connectionFactoryJndiName;
        return this;
    }

    /**
     * 初期化が未実行の場合、初期化を行う。<br/>
     * 既に初期化済みである場合は、何もしない。
     */
    private void prepareIfNecessary() {
        if (!isInitialized()) {
            initialize();
        }
    }

    /**
     * JMSプロバイダの初期化を行う。<br/>
     * {@link ConnectionFactory}および{@link Queue}が設定される。
     */
    private void initialize() {
        // コネクションファクトリをJNDIからルックアップする。
        ConnectionFactory factory = lookUpConnectionFactory();
        setConnectionFactory(factory);
        // キューをルックアップする。
        Map<String, Queue> destinations = lookUpQueues(destinationNamePairs);
        setDestinations(destinations);
    }

    /**
     * プロバイダが初期化済であるか判定する。
     *
     * @return {@link ConnectionFactory}が設定済の時、真
     */
    private boolean isInitialized() {
        return getConnectionFactory() != null;
    }

    /**
     * {@link ConnectionFactory}をJNDIから取得する。<br/>
     * JNDI名には{@link #setConnectionFactoryJndiName(String)}で
     * 設定した値が使用される。
     *
     * @return JNDIから取得した{@link ConnectionFactory}
     */
    private ConnectionFactory lookUpConnectionFactory() {
        if (StringUtil.isNullOrEmpty(connectionFactoryJndiName)) {
            throw new IllegalStateException("connectionFactoryName must be set.");
        }
        if (jndiHelper == null) {
            throw new IllegalStateException("jndiHelper must be set.");
        }
        return jndiHelper.lookUp(connectionFactoryJndiName);
    }

    /**
     * {@link Queue}をJNDIから取得する。<br/>
     * 引数には、key=キュー論理名、value=キューJNDI名をペアにしたMapを渡す。
     * Mapの各エントリからキューのJNDI名を取得しルックアップし、
     * 取得したキューとそのエントリのキュー論理名とを紐付けたMapを返却する。
     *
     * @param pairs key=キュー論理名、value=キューJNDI名をペアにしたMap
     * @return key=キュー論理名、value={@link Queue}をペアにしたMap
     */
    private Map<String, Queue> lookUpQueues(Map<String, String> pairs) {
        Map<String, Queue> result = new HashMap<String, Queue>();
        for (Entry<String, String> pair : pairs.entrySet()) {
            String logicalName = pair.getKey();
            String nameToLookUpInJndi = pair.getValue();
            Queue q = jndiHelper.lookUp(nameToLookUpInJndi);
            result.put(logicalName, q);
        }
        return result;
    }
}
