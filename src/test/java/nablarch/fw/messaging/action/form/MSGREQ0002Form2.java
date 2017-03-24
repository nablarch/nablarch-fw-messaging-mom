package nablarch.fw.messaging.action.form;

import nablarch.core.dataformat.DataRecord;
import nablarch.core.db.statement.autoproperty.CurrentDateTime;
import nablarch.core.db.statement.autoproperty.RequestId;
import nablarch.core.db.statement.autoproperty.UserId;
import nablarch.fw.messaging.RequestMessage;

import java.sql.Timestamp;

/**
 * テスト用のフォームオブジェクト。
 *
 * @author hisaaki sioiri
 */
public class MSGREQ0002Form2 {

    /** 受信メッセージID */
    private String messageId;

    /** 受信日 */
    @CurrentDateTime(format = "yyyyMMdd")
    private String jsnDate;

    /** 漢字名称 */
    private String kanjiName;

    /** カナ名称 */
    private String kanaName;

    /** メールアドレス */
    private String mailAddress;

    /** 内線番号(ビル番号) */
    private String extensionNumberBuilding;

    /** 内線番号(個人番号) */
    private String extensionNumberPersonal;

    /** 登録ユーザID */
    @UserId
    private String insertUserId;

    /** 登録日 */
    @CurrentDateTime
    private Timestamp insertDate;

    /** 実行時ID */
    private String insertExecutionId;

    /** リクエストID */
    @RequestId
    private String insertRequestId;

    /** 更新ユーザID */
    @UserId
    private String updatedUserId;

    /** 更新日 */
    @CurrentDateTime
    private Timestamp updatedDate;

    /**
     * コンストラクタ
     *
     * @param message リクエストメッセージ
     */
    public MSGREQ0002Form2(String messageId, RequestMessage message) {
        this.messageId = messageId;
        DataRecord data = message.getRecordOf("userData");
        kanjiName = data.getString("kanjiName");
        kanaName = data.getString("kanaName");
        mailAddress = data.getString("mailAddress");
        extensionNumberBuilding = data.getString("extensionNumberBuilding");
        extensionNumberPersonal = data.getString("extensionNumberPersonal");
    }

    public String getMessageId() {
        return messageId;
    }

    public String getJsnDate() {
        return jsnDate;
    }

    public String getKanjiName() {
        return kanjiName;
    }

    public String getKanaName() {
        return kanaName;
    }

    public String getMailAddress() {
        return mailAddress;
    }

    public String getExtensionNumberBuilding() {
        return extensionNumberBuilding;
    }

    public String getExtensionNumberPersonal() {
        return extensionNumberPersonal;
    }

    public String getInsertUserId() {
        return insertUserId;
    }

    public Timestamp getInsertDate() {
        return insertDate;
    }

    public String getInsertExecutionId() {
        return insertExecutionId;
    }

    public String getInsertRequestId() {
        return insertRequestId;
    }

    public String getUpdatedUserId() {
        return updatedUserId;
    }

    public Timestamp getUpdatedDate() {
        return updatedDate;
    }
}
