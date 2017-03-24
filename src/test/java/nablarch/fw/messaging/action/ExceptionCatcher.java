package nablarch.fw.messaging.action;

import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;

public class ExceptionCatcher implements Handler<Object, Object>{

    public Object handle(Object data, ExecutionContext context) {
        try {
            return context.handleNext(data);
        } catch (RuntimeException e) {
            thrown = e;
            throw e;
        } catch (Error e) {
            thrown = e;
            throw e;
        }
    }
    private static Throwable thrown = null;
    public static Throwable getThrown() {
        return thrown;
    }
}
