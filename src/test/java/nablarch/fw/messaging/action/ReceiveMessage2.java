package nablarch.fw.messaging.action;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 受信メッセージ2
 */
@Entity
@Table(name = "RECEIVE_MESSAGE_2")
public class ReceiveMessage2 {
    
    public ReceiveMessage2() {
    }
    
	public ReceiveMessage2(String messageId, String jsnDate, String kanjiName,
			String kanaName, String mailAddress,
			String extensionNumberBuilding, String extensionNumberPersonal,
			String insertUserId, Timestamp insertDate,
			String insertExecutionId, String insertRequestId,
			String updatedUserId, Timestamp updatedDate) {
		this.messageId = messageId;
		this.jsnDate = jsnDate;
		this.kanjiName = kanjiName;
		this.kanaName = kanaName;
		this.mailAddress = mailAddress;
		this.extensionNumberBuilding = extensionNumberBuilding;
		this.extensionNumberPersonal = extensionNumberPersonal;
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
    
    @Column(name = "JSN_DATE", length = 8)
    public String jsnDate;
    
    @Column(name = "KANJI_NAME", length = 50)
    public String kanjiName;
    
    @Column(name = "KANA_NAME", length = 50)
    public String kanaName;
    
    @Column(name = "MAIL_ADDRESS", length = 100)
    public String mailAddress;
    
    @Column(name = "EXTENSION_NUMBER_BUILDING", length = 2)
    public String extensionNumberBuilding;
    
    @Column(name = "EXTENSION_NUMBER_PERSONAL", length = 4)
    public String extensionNumberPersonal;
    
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
