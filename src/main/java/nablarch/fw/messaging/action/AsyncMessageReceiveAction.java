package nablarch.fw.messaging.action;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import nablarch.common.idgenerator.IdGenerator;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.action.BatchAction;
import nablarch.fw.messaging.RequestMessage;

/**
 * MQ応答なし受信用アクション。
 * <p/>
 * 本クラスでは、受信したメッセージが保持するリクエストID({@link nablarch.fw.messaging.RequestMessage#getRequestPath()})を元に、
 * 受信テーブルに電文を保存する。
 * <p/>
 * 受信テーブルの構造は、必ず下記構造にすること。
 * <pre>
 * ----------------- -----------------------------------------------------
 * 受信電文連番       主キー
 *                   受信した電文(メッセージ)を一意に識別するためのIDを格納するカラム。
 *                   本カラムに設定する値は、#generateReceivedSequence()にて採番を行う。
 *                   カラムの桁数は、任意の桁数を設定可能となっている。
 * ----------------- -----------------------------------------------------
 * 業務電文部         業務電文を格納するカラムを定義する。
 *                   電文の種類に応じて、業務電文の各項目に対するカラムを定義すれば良い。
 * ----------------- -----------------------------------------------------
 * 共通項目部         各プロジェクトの方式に応じて必要なカラムを定義する。
 *                   たとえば、下記のカラムを定義することが想定される。
 *                   ・登録情報(ユーザID、タイムスタンプ、リクエストID、実行時ID)
 *                   ・更新情報(ユーザID、タイムスタンプ)
 * ----------------- -----------------------------------------------------
 * </pre>
 * <p/>
 * 本クラスは1電文を1レコードとして受信テーブルに保存する場合に利用できる。
 * 1電文を複数レコードとして登録する場合や、複数テーブルに保存する場合は本クラスを継承し
 * #handle(nablarch.fw.messaging.RequestMessage, nablarch.fw.ExecutionContext)や
 * #insertMessageTable(String, Object)をオーバライドすること。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class AsyncMessageReceiveAction extends BatchAction<RequestMessage> {

    /** システムリポジトリ上の設定クラスの格納キー値 */
    private static final String ACTION_SETTINGS_KEY = "asyncMessageReceiveActionSettings";

    /**
     * {@inheritDoc}
     * <p/>
     * 本処理では、メッセージキューから受信した電文オブジェクトを、{@link nablarch.fw.messaging.RequestMessage#getRequestPath()}
     * より取得したリクエストIDに対応する受信テーブルに格納する。
     * <p/>
     * リクエストIDに対応した受信テーブルに格納する際には、下記オブジェクトや定義が必要になるため、
     * リクエストID単位に作成する必要がある。
     * <p/>
     * <ol>
     * <li>リクエストIDに対応したFormクラス</li>
     * <li>受信電文INSERT用のSQL文</li>
     * <li></li>
     * </ol>
     */
    @Override
    public Result handle(RequestMessage inputData, ExecutionContext ctx) {

        // メッセージボディー部を読み込む
        inputData.readRecords();

        String requestId = inputData.getRequestPath();
        // Formオブジェクトを生成し、受信電文をデータベースに登録する。
        Object form = createForm(requestId, inputData);
        insertMessageTable(requestId, form);

        return new Result.Success();
    }

    /**
     * {@inheritDoc}
     * 本実装では、{@link DataReader}の生成は行わない。
     * このため、メッセージキューからメッセージをリードするための{@link DataReader}の設定は、
     * コンポーネント設定ファイル側に行う必要がある。
     */
    @Override
    public DataReader<RequestMessage> createReader(ExecutionContext ctx) {
        return null;
    }

    /**
     * 業務用の受信テーブルに受信電文を登録する。
     *
     * @param requestId リクエストID
     * @param form 登録対象の電文を持ったオブジェクト
     */
    protected void insertMessageTable(String requestId, Object form) {
        AppDbConnection connection = DbConnectionContext.getConnection(
                getSettings().getDbTransactionName());
        ParameterizedSqlPStatement statement = connection
                .prepareParameterizedSqlStatementBySqlId(getSqlResource(
                        requestId));
        statement.executeUpdateByObject(form);
    }

    /**
     * 受信テーブルにINSERTを行うためのFormオブジェクトを生成する。
     * <p/>
     * 生成するフォームクラスのクラス名は、パッケージ名:nablarch.fw.messaging.action.AsyncMessageReceiveActionSettings#getFormClassPackage()
     * から取得したパッケージ名、クラス名:リクエストID + "Form"となる。
     * <p/>
     * また、Formクラスには下記引数を持つコンストラクタを定義し、受信メッセージの内容を保持すること。
     * <oi>
     * <li>受信電文連番:{@link String}</li>
     * <li>受信メッセージ:{@link RequestMessage}</li>
     * </oi>
     *
     * @param requestId リクエストID
     * @param message リクエストメッセージ
     * @return 生成したFormオブジェクト
     */
    protected Object createForm(String requestId, RequestMessage message) {
        String formPackage = getSettings().getFormClassPackage();
        String className = formPackage + '.' + requestId + getSettings()
                .getFormClassSuffix();
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(
                    String.class, RequestMessage.class);
            return constructor.newInstance(generateReceivedSequence(), message);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("form class was not found."
                    + " form class name = %s", className), e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format(
                    "required constructor was not found. "
                            + "required constructor:first parameter is String,"
                            + " second parameter is RequestMessage. form class name = %s",
                    className),
                    e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(String.format(
                    "failed to create form instance. form class name = %s",
                    className), e);
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format(
                    "failed to create form instance. form class name = %s",
                    className), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format(
                    "failed to create form instance. form class name = %s",
                    className), e);
        }
    }

    /**
     * 受信電文連番を採番する。
     * <p/>
     * 受信電文連番採番時には、nablarch.fw.messaging.action.AsyncMessageReceiveActionSettings#getReceivedSequenceFormatter()
     * で取得したフォーマッタを使用して、IDのフォーマットを行う。
     * 採番対象を識別するためのIDは、nablarch.fw.messaging.action.AsyncMessageReceiveActionSettings#getTargetGenerateId()より取得する
     *
     * @return 受信メッセージID
     */
    protected String generateReceivedSequence() {
        AsyncMessageReceiveActionSettings settings = getSettings();
        IdGenerator generator = settings.getReceivedSequenceGenerator();
        return generator.generateId(
                settings.getTargetGenerateId(),
                settings.getReceivedSequenceFormatter());
    }

    /**
     * 本アクションを実行するために必要となる設定値を保持するオブジェクトを取得する。
     * <p/>
     * デフォルト動作では、リポジトリ({@link SystemRepository})から設定オブジェクトを取得する。
     *
     * @return 設定オブジェクト
     */
    protected AsyncMessageReceiveActionSettings getSettings() {
        return SystemRepository.get(ACTION_SETTINGS_KEY);
    }

    /**
     * 電文を受信テーブルに登録するためのINSERT文を表すSQLリソースを取得する。
     * <p/>
     * SQLリソースは、「SQLリソース名」 + "#" + 「SQL_ID」形式となる。
     * <ol>
     * <li>SQLリソースは、「{@link AsyncMessageReceiveActionSettings#getSqlFilePackage()} + /<電文のリクエストID>」であること。</li>
     * <li>SQL_IDは、INSERT_MESSAGEであること</li>
     * </ol>
     *
     * @param requestId リクエストID
     * @return SQLリソース
     */
    private String getSqlResource(String requestId) {
        return getSettings().getSqlFilePackage() + '.' + requestId
                + "#INSERT_MESSAGE";
    }
}

