package nablarch.core.repository.jndi;

import nablarch.core.repository.jndi.MockJndi.Operation;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import java.util.Hashtable;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author T.Kawasaki
 */
public class MockJndiTest {

    /**
     * 通しのテスト。<br/>
     * モックオブジェクトに登録したオブジェクトを、JNDI API経由で取得できること。
     * @throws NamingException 予期しない例外
     */
    @Test
    public void test() throws NamingException {
        final Hashtable<String, String> jndiProperties = MockJndi.getMockJndiProperties();
        
        // JNDIオブジェクトを登録
        MockJndi.prepare().add("test", "test string").doInSandBox(new Operation() {
            public void operate() throws NamingException {
                // ルックアップできること。
                InitialContext initialContext = new InitialContext(jndiProperties);
                String str = (String) initialContext.lookup("test");
                assertThat(str, is("test string"));
            }
        });

        final InitialContext rawContext = new InitialContext(jndiProperties);
        // スコープから外れるとルックアップできないこと
        try {
            String str = (String) rawContext.lookup("test");
        } catch (NameNotFoundException e) {
            return;
        }
        fail();
    }

    /**
     * モック用のJNDIプロパティを取得できること。
     */
    @Test
    public void testGetMockJndiProperties() {
        Hashtable<String, String> jndiProperties = MockJndi.getMockJndiProperties();
        assertThat(jndiProperties.get(Context.INITIAL_CONTEXT_FACTORY),
                   is("nablarch.core.repository.jndi.MockInitialContextFactory"));
    }
}
