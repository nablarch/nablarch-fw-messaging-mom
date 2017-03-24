package nablarch.fw.messaging.action;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * メッセージング用IDジェネレータ
 */
@Entity
@Table(name = "MESSAGING_ID_GENERATE")
public class MessagingIdGenerate {
    
    public MessagingIdGenerate() {
    }
    
    public MessagingIdGenerate(String id, Long no) {
		this.id = id;
		this.no = no;
	}

	@Id
    @Column(name = "ID", length = 2, nullable = false)
    public String id;

    @Column(name = "NO", length = 20, nullable = false)
    public Long no;
}
