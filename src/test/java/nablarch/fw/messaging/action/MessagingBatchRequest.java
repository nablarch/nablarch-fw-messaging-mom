package nablarch.fw.messaging.action;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * メッセージング用バッチリクエスト
 */
@Entity
@Table(name = "MESSAGING_BATCH_REQUEST")
public class MessagingBatchRequest {
    
    public MessagingBatchRequest() {
    }

    public MessagingBatchRequest(String requestId, String requestName, String processHaltFlg,
            String processActiveFlg, String serviceAvailable) {
        this.requestId = requestId;
        this.requestName = requestName;
        this.processHaltFlg = processHaltFlg;
        this.processActiveFlg = processActiveFlg;
        this.serviceAvailable = serviceAvailable;
    }

    @Id
    @Column(name = "REQUEST_ID", length = 10, nullable = false)
    public String requestId;

    @Column(name = "REQUEST_NAME", length = 100, nullable = false)
    public String requestName;

    @Column(name = "PROCESS_HALT_FLG", length = 1, nullable = false)
    public String processHaltFlg;

    @Column(name = "PROCESS_ACTIVE_FLG", length = 1, nullable = false)
    public String processActiveFlg;

    @Column(name = "SERVICE_AVAILABLE", length = 1, nullable = false)
    public String serviceAvailable;
}
