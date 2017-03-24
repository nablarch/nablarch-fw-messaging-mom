package nablarch.core.repository.jndi;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * {@link InitialContextFactory}のモック実装クラス。<br/>
 * ルックアップ時に内部的に保持したオブジェクトを返却する{@link Context}クラスを生成する。
 *
 * @author T.Kawasaki
 */
public class MockInitialContextFactory implements InitialContextFactory {

    /** 本クラスの完全修飾名 */
    public static String FQCN = MockInitialContextFactory.class.getName();

    /** 内部的に保持するオブジェクト */
    private static Map<String, Object> objects = new HashMap<String, Object>();

    /** デフォルトコンストラクタ */
    public MockInitialContextFactory() {
    }

    /** {@inheritDoc} */
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        return new MockContext(objects);
    }

    /**
     * オブジェクトを一括登録する。
     *
     * @param map 登録対象オブジェクトを格納したマップ
     */
    public static void register(Map<String, Object> map) {
        objects = map;
    }

    /** オブジェクトを一括登録解除する。 */
    public static void unRegister() {
        objects = new HashMap<String, Object>();
    }

    /** {@link Context}のモック実装クラス。 */
    private static class MockContext implements Context {
        /** 内部的に保持するオブジェクト */
        private final Map<String, Object> objects;

        /**
         * コンストラクタ。
         *
         * @param objects 内部的に保持するオブジェクト
         */
        private MockContext(Map<String, Object> objects) {
            this.objects = objects;
        }

        /** {@inheritDoc} */
        public Object lookup(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Object lookup(String name) throws NamingException {
            Object result = objects.get(name);
            if (result == null) {
                throw new NameNotFoundException("name not found. name=[" + name + "]");
            }
            return result;
        }

        /** {@inheritDoc} */
        public void bind(Name name, Object obj) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void bind(String name, Object obj) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void rebind(Name name, Object obj) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void rebind(String name, Object obj) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void unbind(Name name) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void unbind(String name) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void rename(Name oldName, Name newName) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public void rename(String oldName, String newName) throws NamingException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public void destroySubcontext(Name name) throws NamingException {
        }

        /** {@inheritDoc} */
        public void destroySubcontext(String name) throws NamingException {
        }

        /** {@inheritDoc} */
        public Context createSubcontext(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Context createSubcontext(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Object lookupLink(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Object lookupLink(String name) throws NamingException {
            return lookup(name);
        }

        /** {@inheritDoc} */
        public NameParser getNameParser(Name name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public NameParser getNameParser(String name) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Name composeName(Name name, Name prefix) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public String composeName(String name, String prefix) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Object addToEnvironment(String propName, Object propVal) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Object removeFromEnvironment(String propName) throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public Hashtable<?, ?> getEnvironment() throws NamingException {
            return null;
        }

        /** {@inheritDoc} */
        public void close() throws NamingException {
        }

        /** {@inheritDoc} */
        public String getNameInNamespace() throws NamingException {
            return null;
        }
    }
}
