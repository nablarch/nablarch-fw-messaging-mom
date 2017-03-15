package nablarch.core.repository.jndi;

import javax.naming.Context;
import javax.naming.NamingException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * JNDIのモッククラス。
 *
 * @author T.Kawasaki
 * @see MockInitialContextFactory
 */
public class MockJndi {

    /** 登録オブジェクト */
    private final Map<String, Object> objects = new HashMap<String, Object>();

    /**
     * モック使用時のJNDIプロパティを取得する。<br/>
     * このプロパティを{@link javax.naming.InitialContext(Hashtable)}に適用すると、
     * モックオブジェクトが使用されるようになる。
     *
     * @return プロパティ
     * @see Context#INITIAL_CONTEXT_FACTORY
     */
    public static Hashtable<String, String> getMockJndiProperties() {
        Hashtable<String, String> jndiProps = new Hashtable<String, String>();
        jndiProps.put(Context.INITIAL_CONTEXT_FACTORY, MockInitialContextFactory.FQCN);
        jndiProps.put(Context.PROVIDER_URL, "http://localhost/");
        return jndiProps;
    }

    /**
     * 準備を開始する。
     *
     * @return モックオブジェクトのインスタンス
     */
    public static MockJndi prepare() {
        return new MockJndi();
    }

    /**
     * オブジェクトを追加する。
     *
     * @param jndiName JNDI名
     * @param obj      オブジェクト
     * @return 本クラスのインスタンス
     */
    public MockJndi add(String jndiName, Object obj) {
        objects.put(jndiName, obj);
        return this;
    }

    public void register() {
        MockInitialContextFactory.register(objects);
    }

    public static void unRegister() {
        MockInitialContextFactory.unRegister();
    }
    /**
     * サンドボックス内で処理を行う。<br/>
     * {@link #add(String, Object)}で追加した設定は、このサンドボックス内でのみ有効となる。
     * スコープ外で再度JNDIルックアップしても、オブジェクトは取得できない。
     *
     * @param op 処理内容
     * @throws NamingException 処理中に発生した{@link NamingException}
     */
    public void doInSandBox(Operation op) throws NamingException {
        register();
        try {
            op.operate();
        } finally {
            unRegister();
        }
    }

    /** サンドボックス内で行う処理を記述するためのインタフェース。 */
    public static interface Operation {
        /**
         * 処理を行う。
         * @throws NamingException 処理中に発生した{@link NamingException}
         */
        void operate() throws NamingException;
    }
}
