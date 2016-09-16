package nablarch.fw.messaging.provider.exception;

import nablarch.core.util.annotation.Published;
import nablarch.fw.handler.retry.Retryable;
import nablarch.fw.messaging.MessagingException;

/**
 * 例外がMOM接続に関する問題である場合に送出される例外。
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public class MomConnectionException extends MessagingException implements Retryable {

    /**
     * コンストラクタ。
     * @param message エラーメッセージ
     * @param cause   起因となる例外
     */
    public MomConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
