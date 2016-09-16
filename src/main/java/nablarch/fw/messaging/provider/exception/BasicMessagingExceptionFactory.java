package nablarch.fw.messaging.provider.exception;

import nablarch.fw.messaging.MessagingException;
import nablarch.fw.messaging.provider.MessagingExceptionFactory;

/**
 * {@link MessagingExceptionFactory}の基本実装クラス。
 * @author Kiyohito Itoh
 */
public class BasicMessagingExceptionFactory implements MessagingExceptionFactory {

    /**
     * {@inheritDoc}
     * </p>
     * {@link MessagingException}を生成する。
     */
    public MessagingException createMessagingException(String message, Throwable cause) {
        return new MessagingException(message, cause);
    }
}
