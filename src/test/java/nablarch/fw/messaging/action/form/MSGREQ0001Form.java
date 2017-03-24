package nablarch.fw.messaging.action.form;

import nablarch.core.dataformat.DataRecord;
import nablarch.core.db.statement.autoproperty.CurrentDateTime;
import nablarch.core.db.statement.autoproperty.RequestId;
import nablarch.core.db.statement.autoproperty.UserId;
import nablarch.fw.messaging.RequestMessage;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * テスト用のフォームオブジェクト
 *
 * @author hisaaki sioiri
 */
public class MSGREQ0001Form {

    /** 受信メッセージID */
    private String messageId;

    /** 受信日 */
    @CurrentDateTime(format = "yyyyMMdd")
    private String jsnDate;

    /** 契約番号 */
    private String keiNo;

    /** 商品コード1 */
    private String itemCode1;

    /** 商品名1 */
    private String itemName1;

    /** 金額1 */
    private BigDecimal itemAmount1;

    /** 商品コード2 */
    private String itemCode2;

    /** 商品名2 */
    private String itemName2;

    /** 金額2 */
    private BigDecimal itemAmount2;

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
    public MSGREQ0001Form(String messageId, RequestMessage message) {
        this.messageId = messageId;
        DataRecord data = message.getRecordOf("Data");
        keiNo = data.getString("keiNo");
        itemCode1 = data.getString("itemCode1");
        itemName1 = data.getString("itemName1");
        itemAmount1 = data.getBigDecimal("itemAmount1");
        itemCode2 = data.getString("itemCode2");
        itemName2 = data.getString("itemName2");
        itemAmount2 = data.getBigDecimal("itemAmount2");
    }

    public String getMessageId() {
        return messageId;
    }

    public String getJsnDate() {
        return jsnDate;
    }

    public String getKeiNo() {
        return keiNo;
    }

    public String getItemCode1() {
        return itemCode1;
    }

    public String getItemName1() {
        return itemName1;
    }

    public BigDecimal getItemAmount1() {
        return itemAmount1;
    }

    public String getItemCode2() {
        return itemCode2;
    }

    public String getItemName2() {
        return itemName2;
    }

    public BigDecimal getItemAmount2() {
        return itemAmount2;
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

