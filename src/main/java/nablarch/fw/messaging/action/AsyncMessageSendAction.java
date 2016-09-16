package nablarch.fw.messaging.action;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import nablarch.core.dataformat.DataRecordFormatter;
import nablarch.core.dataformat.FormatterFactory;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.FilePathSetting;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import nablarch.fw.action.BatchAction;
import nablarch.fw.launcher.CommandLine;
import nablarch.fw.messaging.FwHeader;
import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.SendingMessage;
import nablarch.fw.reader.DatabaseRecordReader;

/**
 * MQ応答なし送信用の共通アクション。
 * <p/>
 * 本クラスでは、送信用のテーブル（一時テーブル）から送信対象のデータを取得し、メッセージを送信する。
 * <p/>
 * 送信対象のメッセージのリクエストIDは、本バッチの起動時の引数(起動パラメータ名:messageRequestId)として指定すること。
 * <p/>
 * 送信対象のデータを抽出するSQL文は、テーブル単位に用意する必要がある。
 * 詳細は、{@link nablarch.fw.messaging.action.AsyncMessageSendAction#createStatement()}を参照
 * <p/>
 * メッセージが正常に送信できた場合には、#transactionNormalEnd(nablarch.core.db.statement.SqlRow, nablarch.fw.ExecutionContext)にて
 * 対象データのステータスを処理済みに更新する。
 * <p/>
 * メッセージ送信時に例外が発生した場合には、#transactionAbnormalEnd(Throwable, nablarch.core.db.statement.SqlRow, nablarch.fw.ExecutionContext)にて
 * 対象データのステータスをエラーに更新する。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class AsyncMessageSendAction extends BatchAction<SqlRow> {

    /** システムリポジトリ上の設定クラスの格納キー値 */
    private static final String ACTION_SETTINGS_KEY = "asyncMessageSendActionSettings";
    
    /** 電文フォーマット定義ファイルのサフィックス */
    private static final String LAYOUT_FILE_NAME_SUFFIX = "_SEND";

    /** Formクラスのインスタンスを生成するためのコンストラクタ */
    private Constructor<?> formConstructor;

    /** 送信メッセージのメッセージリクエストID */
    private String sendMessageRequestId;
    

    /**
     * 初期処理を行う。
     * <p/>
     * 起動引数から送信対象のメッセージのリクエストIDを取得し、保持する。
     */
    @Override
    protected void initialize(CommandLine command, ExecutionContext context) { // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
        sendMessageRequestId = command.getParam("messageRequestId");
    }

    /**
     * 入力データからヘッダ部及び業務データ部からなるメッセージオブジェクトを生成し、
     * 送信処理（キューへのPUT）を行う。
     * <p/>
     * 処理詳細は、以下のとおり。
     * <ol>
     * <li>ヘッダ部は、{@link #createHeaderRecord(nablarch.core.db.statement.SqlRow)}で生成する。</li>
     * <li>業務データ部は、インプットデータ(本メソッドの引数)をそのまま使用する。</li>
     * <li>送信先のキューは、{@link #getQueueName()}から取得する。</li>
     * </ol>
     */
    @Override // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
    public Result handle(SqlRow inputData, ExecutionContext ctx) { // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
        SendingMessage message = new SendingMessage();

        message.setFormatter(createHeaderRecordFormatter());
        message.addRecord(createHeaderRecord(inputData));

        message.setFormatter(createDataRecordFormatter());
        message.addRecord(inputData);

        message.setDestination(getQueueName());
        MessagingContext context = MessagingContext.getInstance();
        context.send(message);

        return new Result.Success();
    }

    /**
     * インプットテーブルの対象レコードのステータスを処理済みに更新する。
     * これにより、次回対象データ抽出時に処理済みのレコードは対象外となり、
     * 2重送信を防止する事が出来る。
     * <p/>
     * ステータスを更新するSQL文は、{@link #getSqlResource()}で取得した
     * SQLリソース内に記述されたSQL_ID=UPDATE_NORMAL_ENDを使用する。
     */
    @Override
    public void transactionNormalEnd(SqlRow inputData, ExecutionContext ctx) { // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
        updateStatus(inputData, "UPDATE_NORMAL_END");
    }

    /**
     * インプットテーブルの対象レコードのステータスをエラーに更新する。
     * これにより、エラーレコードを再度送信することを防止する事ができる。
     * <p/>
     * （エラーレコードは、何度送信してもエラーになることが考えられるため
     * ステータスをエラーに変更する必要がある。)
     * <p/>
     * ステータスを更新するSQL文は、{@link #getSqlResource()}で取得した
     * SQLリソース内に記述されたSQL_ID=UPDATE_ABNORMAL_ENDを使用する。
     */
    @Override
    public void transactionAbnormalEnd(Throwable e, SqlRow inputData, ExecutionContext ctx) { // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
        updateStatus(inputData, "UPDATE_ABNORMAL_END");
    }

    /**
     * ステータスを更新する。
     * <p/>
     * 指定されたインプットデータ、SQL_IDを元にステータスを更新する。
     * ステータスを更新するためのFormクラスは、nablarch.fw.messaging.action.AsyncMessageSendActionSettings#getFormClassName()
     * から取得したFormクラスを使用して行う。
     *
     * @param inputData インプットデータ
     * @param sqlId SQL_ID
     */
    protected void updateStatus(SqlRow inputData, String sqlId) {
        Object instance = createFormInstance(inputData);
        AppDbConnection connection = DbConnectionContext.getConnection(
                getTransactionName());
        ParameterizedSqlPStatement statement = connection
                .prepareParameterizedSqlStatementBySqlId(
                        getSqlResource() + '#' + sqlId);
        int updateCount = statement.executeUpdateByObject(instance);
        if (updateCount != 1) {
            throw new IllegalStateException(
                    "update data was not single record. updated record count = "
                            + updateCount);
        }
    }

    /**
     * 送信対象のデータを抽出するための{@link nablarch.fw.reader.DatabaseRecordReader}を生成する。
     * {@link nablarch.fw.reader.DatabaseRecordReader}生成時に指定する{@link SqlPStatement}は、
     * #createStatement()により生成する。
     */
    @Override // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
    public DataReader<SqlRow> createReader(ExecutionContext ctx) { // SUPPRESS CHECKSTYLE @OverrideでJavaDocは継承されるので除外
        DatabaseRecordReader reader = new DatabaseRecordReader();
        reader.setStatement(createStatement());
        return reader;
    }

    /**
     * インプットデータを抽出するための{@link SqlPStatement}を生成する。
     * <p/>
     * {@link SqlPStatement}を生成するためのSQLは、下記ルールに従い取得する。
     * <ui>
     * <li>SQLリソースは、{@link #getSqlResource()}の実装に準拠する</li>
     * <li>SQL_IDは、SELECT_SEND_DATA固定</li>
     * </ui>
     *
     * @return {@link SqlPStatement}
     */
    protected SqlPStatement createStatement() {
        AppDbConnection connection = DbConnectionContext.getConnection(
                getTransactionName());
        return connection.prepareStatementBySqlId(
                getSqlResource()
                        + "#SELECT_SEND_DATA");
    }

    /**
     * SQLリソース名称を取得する。
     * <p/>
     * 返却するSQLリソース名称は、「nablarch.fw.messaging.action.AsyncMessageSendActionSettings#getSqlFilePackage() + "." + バッチ起動時に指定した送信メッセージのメッセージリクエストID」となる。
     *
     * @return SQLリソース名称
     * @see nablarch.fw.messaging.action.AsyncMessageSendActionSettings#getSqlFilePackage()
     */
    protected String getSqlResource() {
        return getSettings().getSqlFilePackage() + '.' + sendMessageRequestId;
    }

    /**
     * 本アクションを実行するために必要となる設定値を保持するオブジェクトを取得する。
     * <p/>
     * デフォルト動作では、リポジトリ({@link SystemRepository})から設定オブジェクトを取得する。
     *
     * @return 設定オブジェクト
     */
    protected AsyncMessageSendActionSettings getSettings() {
        return SystemRepository.get(ACTION_SETTINGS_KEY);
    }

    /**
     * ヘッダ部のフォーマットを生成する。
     * <p/>
     * ヘッダ部を表すフォーマット定義ファイル名は、{@link #getHeaderFormatName()}より取得する。
     *
     * @return 生成したフォーマッタ
     */
    protected DataRecordFormatter createHeaderRecordFormatter() {
        FormatterFactory formatterFactory = FormatterFactory.getInstance();
        return formatterFactory.createFormatter(
                FilePathSetting.getInstance().getFileWithoutCreate(
                        getFormatDir(), getHeaderFormatName()));
    }

    /**
     * ヘッダ部のフォーマット定義ファイル名を取得する。
     * <p/>
     * ヘッダ部のフォーマット定義ファイル名は、nablarch.fw.messaging.action.AsyncMessageSendActionSettings#getHeaderFormatName()
     * から取得した値となる。
     *
     * @return フォーマット定義ファイル名
     * @see nablarch.fw.messaging.action.AsyncMessageSendActionSettings#getHeaderFormatName()
     */
    protected String getHeaderFormatName() {
        return getSettings().getHeaderFormatName();
    }

    /**
     * ヘッダデータを生成する。
     * <p/>
     * ヘッダ部に設定する項目は以下のとおり。
     * <ul>
     * <li>リクエストIDを設定するフィールド(項目名:requestId)があること。
     * リクエストID部には、バッチ起動時に指定された送信メッセージのメッセージIDを設定する。
     * </li>
     * <li>リクエストID以外の項目は、任意の項目を設定することが可能である。
     * 設定する項目は、nablarch.fw.messaging.action.AsyncMessageSendActionSettings#getHeaderItemList()から取得した項目となり、
     * 設定する値は{@link #handle(nablarch.core.db.statement.SqlRow, nablarch.fw.ExecutionContext)}のインプットデータから取得する。
     * </li>
     * </ul>
     *
     * @param inputData 入力データ
     * @return 生成したヘッダ情報
     */
    protected Map<String, Object> createHeaderRecord(SqlRow inputData) {
        FwHeader fwHeader = new FwHeader();
        fwHeader.setRequestId(sendMessageRequestId);

        List<String> itemList = getSettings().getHeaderItemList();
        for (String item : itemList) {
            Object data = inputData.get(item);
            fwHeader.put(item, data);
        }
        return fwHeader;
    }

    /**
     * データ部のフォーマッタ生成する。
     *
     * @return 生成したフォーマッタ
     */
    protected DataRecordFormatter createDataRecordFormatter() {
        FormatterFactory formatterFactory = FormatterFactory.getInstance();
        return formatterFactory.createFormatter(
                FilePathSetting.getInstance().getFileWithoutCreate(getFormatDir(),
                        sendMessageRequestId + LAYOUT_FILE_NAME_SUFFIX));
    }

    /**
     * 送信キュー名を取得する。
     *
     * @return 送信キュー名
     */
    protected String getQueueName() {
        return getSettings().getQueueName();
    }

    /**
     * フォーマット定義ファイルの配置ディレクトリを示す論理名を取得する。
     *
     * @return フォーマット定義ファイルの配置ディレクトリ(論理名)
     */
    protected String getFormatDir() {
        return getSettings().getFormatDir();
    }


    /**
     * 送信用一時テーブルを更新するためのFormオブジェクトを生成する。
     *
     * @param inputData Formインスタンスを生成するためのインプットデータ
     * @return 生成したFormクラスのインスタンス
     */
    protected synchronized Object createFormInstance(Map<String, ?> inputData) {
        String className = getSettings().getFormClassName();
        try {
            if (formConstructor == null) {
                Class<?> clazz = Class.forName(
                        className);
                formConstructor = clazz.getConstructor(Map.class);
            }
            return formConstructor.newInstance(inputData);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("form class was not found."
                    + " form class name = %s", className), e);
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format(
                    "failed to create form instance. form class name = %s",
                    className), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format(
                    "failed to create form instance. form class name = %s",
                    className), e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(String.format(
                    "failed to create form instance. form class name = %s",
                    className), e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format(
                    "required constructor was not found. "
                            + "required constructor:parameter is Map."
                            + " form class name = %s",
                    className), e);
        }
    }

    /**
     * トランザクション名を取得する。
     *
     * @return トランザクション名
     */
    private String getTransactionName() {
        return getSettings().getTransactionName();
    }

}

