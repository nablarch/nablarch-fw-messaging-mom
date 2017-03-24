package nablarch.fw.messaging.provider;

import nablarch.core.repository.jndi.JndiHelper;
import nablarch.core.repository.jndi.MockJndi;
import nablarch.fw.messaging.MessagingProvider;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;

/**
 * @author T.Kawasaki
 */
public class JndiLookingUpJmsMessagingProviderTest extends JmsMessagingProviderTest {

    private static Map<String, String> pair = new HashMap<String, String>() {{
        put("LOCAL.RECEIVE", "LOCAL.RECEIVE");
        put("LOCAL.RECEIVE.DLQ", "LOCAL.RECEIVE.DLQ");
        put("SERVICE1.REQUEST", "SERVICE1.REQUEST");
        put("SERVICE1.REQUEST.DLQ", "SERVICE1.REQUEST.DLQ");
        put("SERVICE2.REQUEST", "SERVICE2.REQUEST");
        put("DEFAULT.DLQ", "DEFAULT.DLQ");
    }};

    @Override
    protected MessagingProvider createProvider() {

        // JNDI登録
        MockJndi.prepare()
                .add("connectionFactoryJndiName", new StubConnectionFactory())
                .add("LOCAL.RECEIVE", new StubQueue("LOCAL.RECEIVE"))
                .add("LOCAL.RECEIVE.DLQ", new StubQueue("LOCAL.RECEIVE.DLQ"))
                .add("SERVICE1.REQUEST", new StubQueue("SERVICE1.REQUEST"))
                .add("SERVICE1.REQUEST.DLQ", new StubQueue("SERVICE1.REQUEST.DLQ"))
                .add("SERVICE2.REQUEST", new StubQueue("SERVICE2.REQUEST"))
                .add("DEFAULT.DLQ", new StubQueue("DEFAULT.DLQ"))
                .register();

        JndiHelper helper = new JndiHelper();
        helper.setJndiProperties(MockJndi.getMockJndiProperties());

        return new JndiLookingUpJmsMessagingProvider()
                .setConnectionFactoryJndiName("connectionFactoryJndiName")
                .setJndiHelper(helper)
                .setDestinationNamePairs(pair);
    }

    @Override
    public void testGetConnectionFactory() throws Exception {
        // サブクラスでは不要なテストなのでオーバライド
    }

    @After
    public void clearContext() {
        MockJndi.unRegister();
    }
}
