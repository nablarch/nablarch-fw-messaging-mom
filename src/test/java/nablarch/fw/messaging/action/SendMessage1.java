package nablarch.fw.messaging.action;

import java.sql.Timestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 送信メッセージ1
 */
@Entity
@Table(name = "SEND_MESSAGE_1")
public class SendMessage1 {
    
    public SendMessage1() {
    }
    
	public SendMessage1(String messageId, String keiNo, String itemCode1,
			String itemName1, Long itemAmount1, String itemCode2,
			String itemName2, Long itemAmount2, String status,
			String insertUserId, Timestamp insertDate,
			String insertExecutionId, String insertRequestId,
			String updatedUserId, Timestamp updatedDate) {
		this.messageId = messageId;
		this.keiNo = keiNo;
		this.itemCode1 = itemCode1;
		this.itemName1 = itemName1;
		this.itemAmount1 = itemAmount1;
		this.itemCode2 = itemCode2;
		this.itemName2 = itemName2;
		this.itemAmount2 = itemAmount2;
		this.status = status;
		this.insertUserId = insertUserId;
		this.insertDate = insertDate;
		this.insertExecutionId = insertExecutionId;
		this.insertRequestId = insertRequestId;
		this.updatedUserId = updatedUserId;
		this.updatedDate = updatedDate;
	}

	@Id
    @Column(name = "MESSAGE_ID", length = 20, nullable = false)
    public String messageId;
    
    @Column(name = "KEI_NO", length = 10)
    public String keiNo;
    
    @Column(name = "ITEM_CODE_1", length = 4)
    public String itemCode1;
    
    @Column(name = "ITEM_NAME_1", length = 200)
    public String itemName1;
    
    @Column(name = "ITEM_AMOUNT_1", length = 20)
    public Long itemAmount1;
    
    @Column(name = "ITEM_CODE_2", length = 4)
    public String itemCode2;
    
    @Column(name = "ITEM_NAME_2", length = 200)
    public String itemName2;
    
    @Column(name = "ITEM_AMOUNT_2", length = 20)
    public Long itemAmount2;
    
    @Column(name = "STATUS", length = 1)
    public String status;
    
    @Column(name = "INSERT_USER_ID", length = 10)
    public String insertUserId;
    
    @Column(name = "INSERT_DATE")
    public Timestamp insertDate;
    
    @Column(name = "INSERT_EXECUTION_ID", length = 21)
    public String insertExecutionId;
    
    @Column(name = "INSERT_REQUEST_ID", length = 10)
    public String insertRequestId;
    
    @Column(name = "UPDATED_USER_ID", length = 10)
    public String updatedUserId;
    
    @Column(name = "UPDATED_DATE")
    public Timestamp updatedDate;
}
