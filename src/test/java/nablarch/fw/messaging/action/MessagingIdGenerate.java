package nablarch.fw.messaging.action;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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
