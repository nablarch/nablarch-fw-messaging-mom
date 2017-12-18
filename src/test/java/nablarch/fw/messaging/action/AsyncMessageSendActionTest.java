package nablarch.fw.messaging.action;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import nablarch.core.ThreadContext;
import nablarch.core.dataformat.DataRecord;
import nablarch.core.dataformat.DataRecordFormatter;
import nablarch.core.dataformat.FormatterFactory;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.FilePathSetting;
import nablarch.fw.ExecutionContext;
import nablarch.fw.launcher.CommandLine;
import nablarch.fw.launcher.Main;
import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.MessagingProvider;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link AsyncMessageSendAction}のテスト。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class AsyncMessageSendActionTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/fw/messaging/action/AsyncMessageSendActionTest.xml");

    /** {@link MessagingContext} */
    private static MessagingContext context;

    /**
     * 本テストクラスのセットアップ処理
     * <p/>
     * テストで使用するテーブルの構築を行う。
     */
    @BeforeClass
    public static void classSetUp() {
        ThreadContext.clear();

        VariousDbTestHelper.createTable(SendMessage1.class);
        VariousDbTestHelper.createTable(SendMessage2.class);

    }

    private ExecutorService executorService;
    
    @Before
    public void setUp() {
        VariousDbTestHelper.dropTable(MessagingBatchRequest.class);
        VariousDbTestHelper.createTable(MessagingBatchRequest.class);
        VariousDbTestHelper.setUpTable(
                new MessagingBatchRequest("R000000001", "リクエスト０１", "0", "0", "1"),
                new MessagingBatchRequest("R000000002", "リクエスト０２", "0", "0", "1"),
                new MessagingBatchRequest("R000000003", "リクエスト０３", "0", "0", "1"),
                new MessagingBatchRequest("R000000004", "リクエスト０４", "0", "0", "1"));
        if (context != null) {
            context.close();
        }
        executorService = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdownNow();
    }

    /**
     * テスト用の電文送信プロセスを実行する。
     *
     * @param requestId リクエストID
     * @param userId ユーザID
     */
    private CompletionService<Integer> executeSendThread(
            final String requestId,
            final String userId,
            final String sendMessageRequestId) throws Exception {
        VariousDbTestHelper.setUpTable(
                new MessagingBatchRequest("R000000001", "リクエスト０１", "0", "0", "1"),
                new MessagingBatchRequest("R000000002", "リクエスト０２", "0", "0", "1"),
                new MessagingBatchRequest("R000000003", "リクエスト０３", "0", "0", "1"),
                new MessagingBatchRequest("R000000004", "リクエスト０４", "0", "0", "1"));

        CompletionService<Integer> service =
                new ExecutorCompletionService<Integer>(executorService);

        service.submit(new Callable<Integer>() {
            public Integer call() throws Exception {
                CommandLine commandLine = new CommandLine(
                        "-diConfig",
                        "nablarch/fw/messaging/action/AsyncMessageSendActionTest.xml",
                        "-userId", userId,
                        "-requestPath", "AsyncMessageSendAction/" + requestId,
                        "-messageRequestId", sendMessageRequestId);
                return Main.execute(commandLine);
            }
        });
        return service;
    }

    /**
     * プロセスを停止する。
     *
     * @param requestId リクエストID
     */
    private static void stopProcess(
            final String requestId) throws InterruptedException {

        Thread.sleep(5000);

        MessagingBatchRequest entity = VariousDbTestHelper.findById(MessagingBatchRequest.class, requestId);
        entity.processHaltFlg = "1";
        VariousDbTestHelper.update(entity);

        // プロセスアクティブフラグがONになるまで待機
        int count = 0;
        while (true) {
            MessagingBatchRequest query = VariousDbTestHelper.findById(MessagingBatchRequest.class, requestId);
            if (query == null) {
                break;
            }
            if ("0".equals(query.processActiveFlg)) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (count++ > 100) {
                throw new RuntimeException("受信スレッドが停止しません！！");
            }
        }
    }

    /**
     * {@link AsyncMessageSendAction}のテスト。
     * 正常に電文を送信できるケースのテストを実施
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendMessage1() throws Exception {

        //**********************************************************************
        // テストデータ準備
        //**********************************************************************
        VariousDbTestHelper.setUpTable(
                new SendMessage1("00000000000000000001", "1111111111", "0001", "アイテム１０", 100L, null, null, null, "0",
                        null, null, null, null, null, null),
                new SendMessage1("00000000000000000002", "1111111112", "0001", "アイテム１１", 1000L, null, null, null, "0",
                        null, null, null, null, null, null),
                new SendMessage1("00000000000000000003", "1111111112", "0011", "アイテム１２", 10000L, null, null, null, "0",
                        null, null, null, null, null, null));

        //**********************************************************************
        // 送信アクション用の設定
        //**********************************************************************
        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setSqlFilePackage("nablarch.fw.messaging.action.sql");
        settings.setQueueName("SEND.TEST");
        settings.setHeaderFormatName("header.test");
        settings.setFormatDir("format");
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.SendTempForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        settings.setHeaderItemList(new ArrayList<String>() {{
            add("messageId");
        }});
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        //**********************************************************************
        // 送信スレッドを起動する。
        //**********************************************************************
        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID
        CompletionService<Integer> service = executeSendThread(requestId,
                userId, "MSGREQ0003");

        //**********************************************************************
        // 送信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);

        //**********************************************************************
        // 1電文目のアサート
        //**********************************************************************
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");
        context = messagingProvider.createContext();
        ReceivedMessage message = context.receiveSync("SEND.TEST", 10000);
        message.setFormatter(createFormatter("header.test"));
        DataRecord record = message.readRecord();

        // ヘッダ部のアサート
        assertThat(record.getString("requestId"), is("MSGREQ0003"));
        assertThat(record.getString("messageId"), is("00000000000000000001"));

        // ボディ部のアサート
        message.setFormatter(createFormatter("MSGREQ0003"));
        record = message.readRecord();
        assertThat(record.getString("KEI_NO"), is("1111111111"));
        assertThat(record.getString("ITEM_CODE_1"), is("0001"));
        assertThat(record.getString("ITEM_NAME_1"), is("アイテム１０"));
        assertThat(record.getBigDecimal("ITEM_AMOUNT_1")
                         .intValue(), is(100));
        assertThat(record.getString("ITEM_CODE_2"), is(nullValue()));
        assertThat(record.getString("ITEM_NAME_2"), is(nullValue()));
        assertThat(record.getBigDecimal("ITEM_AMOUNT_2")
                         .intValue(), is(0));

        //**********************************************************************
        // 2電文目のアサート
        //**********************************************************************
        message = context.receiveSync("SEND.TEST", 5000);
        message.setFormatter(createFormatter("header.test"));
        record = message.readRecord();

        // ヘッダ部のアサート
        assertThat(record.getString("requestId"), is("MSGREQ0003"));
        assertThat(record.getString("messageId"), is("00000000000000000002"));
        // ボディ部のアサート
        message.setFormatter(createFormatter("MSGREQ0003"));
        record = message.readRecord();
        assertThat(record.getString("KEI_NO"), is("1111111112"));
        assertThat(record.getString("ITEM_CODE_1"), is("0001"));
        assertThat(record.getString("ITEM_NAME_1"), is("アイテム１１"));
        assertThat(record.getBigDecimal("ITEM_AMOUNT_1")
                         .intValue(), is(1000));
        assertThat(record.getString("ITEM_CODE_2"), is(nullValue()));
        assertThat(record.getString("ITEM_NAME_2"), is(nullValue()));
        assertThat(record.getBigDecimal("ITEM_AMOUNT_2")
                         .intValue(), is(0));

        //**********************************************************************
        // 3電文目のアサート
        //**********************************************************************
        message = context.receiveSync("SEND.TEST", 5000);
        message.setFormatter(createFormatter("header.test"));
        record = message.readRecord();

        // ヘッダ部のアサート
        assertThat(record.getString("requestId"), is("MSGREQ0003"));
        assertThat(record.getString("messageId"), is("00000000000000000003"));

        // ボディ部のアサート
        message.setFormatter(createFormatter("MSGREQ0003"));
        record = message.readRecord();
        assertThat(record.getString("KEI_NO"), is("1111111112"));
        assertThat(record.getString("ITEM_CODE_1"), is("0011"));
        assertThat(record.getString("ITEM_NAME_1"), is("アイテム１２"));
        assertThat(record.getBigDecimal("ITEM_AMOUNT_1")
                         .intValue(), is(10000));
        assertThat(record.getString("ITEM_CODE_2"), is(nullValue()));
        assertThat(record.getString("ITEM_NAME_2"), is(nullValue()));
        assertThat(record.getBigDecimal("ITEM_AMOUNT_2")
                         .intValue(), is(0));

        //**********************************************************************
        // 4電文目のアサート
        // 送信した電文数は3なので、4電文目はnullが取得される。
        //**********************************************************************
        message = context.receiveSync("SEND.TEST", 1000);
        assertThat(message, nullValue());

        // 元テーブルのステータスが全て送信済み('1')に変更されていることを確認する。
        List<SendMessage1> list = VariousDbTestHelper.findAll(SendMessage1.class);
        assertThat("レコード数は3", list.size(), is(3));
        for (SendMessage1 row : list) {
            assertThat((String) row.status, is("1"));
        }
    }

    /**
     * {@link AsyncMessageSendAction}のテスト。
     * 電文の送信に失敗するケースを実施
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendMessage2() throws Exception {

        //**********************************************************************
        // テストデータ準備
        //**********************************************************************
        VariousDbTestHelper.setUpTable(
                new SendMessage2("00000000000000000001", "漢字１", "ｶﾅ1", "1@mail", "01", "0001", "0", null, null, null,
                        null, null, null),
                new SendMessage2("00000000000000000002", "漢字２",
                        "ｶﾅ000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                        "2@mail", "02", "0002", "0", null, null, null, null, null, null),
                new SendMessage2("00000000000000000003", "漢字３", "ｶﾅ3", "3@mail", "03", "0003", "0", null, null, null,
                        null, null, null));

        //**********************************************************************
        // 送信アクション用の設定
        //**********************************************************************
        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setSqlFilePackage("nablarch.fw.messaging.action.sql");
        settings.setQueueName("SEND.TEST");
        settings.setHeaderFormatName("header");
        settings.setFormatDir("format");
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.SendTempForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        //**********************************************************************
        // 送信スレッドを起動する。
        //**********************************************************************
        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID
        CompletionService<Integer> service = executeSendThread(requestId,
                userId, "MSGREQ0004");


        //**********************************************************************
        // 送信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);

        //**********************************************************************
        // 1電文目のアサート
        //**********************************************************************
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");
        context = messagingProvider.createContext();
        ReceivedMessage message = context.receiveSync("SEND.TEST", 5000);
        message.setFormatter(createFormatter("header"));
        DataRecord record = message.readRecord();

        // ヘッダ部のアサート
        assertThat(record.getString("requestId"), is("MSGREQ0004"));

        // ボディ部のアサート
        message.setFormatter(createFormatter("MSGREQ0004"));
        record = message.readRecord();
        assertThat(record.getString("KANA_NAME"), is("ｶﾅ1"));
        assertThat(record.getString("KANJI_NAME"), is("漢字１"));
        assertThat(record.getString("MAIL_ADDRESS"), is("1@mail"));
        assertThat(record.getString("EXTENSION_NUMBER_BUILDING"), is("01"));
        assertThat(record.getString("EXTENSION_NUMBER_PERSONAL"), is("0001"));

        //**********************************************************************
        // 2電文目のアサート
        // 2電文目で例外が発生するため、2電文目はnullが取得される。
        //**********************************************************************
        message = context.receiveSync("SEND.TEST", 1000);
        assertThat(message, nullValue());

        // 元テーブルのステータスが全て送信済み('1')に変更されていることを確認する。
        List<SendMessage2> list = VariousDbTestHelper.findAll(SendMessage2.class, "messageId");
        assertThat("1レコード目は、正常に処理できているのでステータスは'1'", list.get(0).status, is("1"));
        assertThat("2レコード目は、例外が発生したのでステータスは'9'", list.get(1).status, is("9"));
        assertThat("3レコード目は、処理されていないのでステータスは'0'のまま", list.get(2).status, is("0"));
    }

    /**
     * {@link AsyncMessageSendAction}のテスト。
     * 電文の送信に失敗するケースを実施
     *
     * @throws InterruptedException
     */
    @Test
    public void testSendMessage3() throws Exception {

        //**********************************************************************
        // テストデータ準備
        //**********************************************************************
        VariousDbTestHelper.setUpTable(
                new SendMessage2("00000000000000000001", "𪛊𪛊🙀🙀", "ｶﾅ1", "1@mail", "01", "0001", "0", null, null, null,
                        null, null, null));

        //**********************************************************************
        // 送信アクション用の設定
        //**********************************************************************
        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setSqlFilePackage("nablarch.fw.messaging.action.sql");
        settings.setQueueName("SEND.TEST");
        settings.setHeaderFormatName("header");
        settings.setFormatDir("format");
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.SendTempForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        //**********************************************************************
        // 送信スレッドを起動する。
        //**********************************************************************
        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID
        CompletionService<Integer> service = executeSendThread(requestId,
                userId, "MSGREQ0004");


        //**********************************************************************
        // 送信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);

        //**********************************************************************
        // 1電文目のアサート
        //**********************************************************************
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");
        context = messagingProvider.createContext();
        ReceivedMessage message = context.receiveSync("SEND.TEST", 5000);
        message.setFormatter(createFormatter("header"));
        DataRecord record = message.readRecord();

        // ヘッダ部のアサート
        assertThat(record.getString("requestId"), is("MSGREQ0004"));

        // ボディ部のアサート
        message.setFormatter(createFormatter("MSGREQ0004"));
        record = message.readRecord();
        assertThat(record.getString("KANA_NAME"), is("ｶﾅ1"));
        assertThat(record.getString("KANJI_NAME"), is("𪛊𪛊🙀🙀"));
        assertThat(record.getString("MAIL_ADDRESS"), is("1@mail"));
        assertThat(record.getString("EXTENSION_NUMBER_BUILDING"), is("01"));
        assertThat(record.getString("EXTENSION_NUMBER_PERSONAL"), is("0001"));

    }

    /** Formクラスが存在しない場合のテスト。 */
    @Test
    public void testFormClassNotFound() {

        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.FormNotFound");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        AsyncMessageSendAction action = new AsyncMessageSendAction();
        try {
            action.createFormInstance(new HashMap<String, Object>());
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is(
                    "form class was not found. form class name = "
                            + "nablarch.fw.messaging.action.form.FormNotFound"));
        }
    }

    /** Formのコンストラクタ定義が不正な場合。 */
    @Test
    public void testFormInvalidConstructor() {

        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.INVALID_CONSTRUCTORForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        AsyncMessageSendAction action = new AsyncMessageSendAction();
        try {
            action.createFormInstance(new HashMap<String, Object>());
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("required constructor was not found."
                    + " required constructor:parameter is Map."
                    + " form class name = nablarch.fw.messaging.action.form.INVALID_CONSTRUCTORForm"));
        }
    }

    /** Formのインスタンス化時に例外が発生する場合 */
    @Test
    public void testFormInstantiationError() {
        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.ErrorConstructorForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        AsyncMessageSendAction action = new AsyncMessageSendAction();
        try {
            action.createFormInstance(new HashMap<String, Object>());
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is(
                    "failed to create form instance. form class name = "
                            + "nablarch.fw.messaging.action.form.ErrorConstructorForm"));
        }
    }

    /** FormがAbstractクラスの場合 */
    @Test
    public void testFormIsAbstract() {
        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.AbstractForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        AsyncMessageSendAction action = new AsyncMessageSendAction();
        try {
            action.createFormInstance(new HashMap<String, Object>());
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is(
                    "failed to create form instance. form class name = "
                            + "nablarch.fw.messaging.action.form.AbstractForm"));
        }
    }

    /** ステータス更新時に更新対象のレコードが存在しない場合。 */
    @Test
    public void testStatusUpdateError() {
        final AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setFormClassName(
                "nablarch.fw.messaging.action.form.SendTempForm");
        settings.setTransactionName(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        settings.setTransactionName("hogeTran");
        settings.setSqlFilePackage("nablarch.fw.messaging.action.sql");
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        SimpleDbTransactionManager tran = repositoryResource.getComponent("tran");
        tran.setDbTransactionName("hogeTran");
        new SimpleDbTransactionExecutor<Void>(tran) {
            @Override
            public Void execute(AppDbConnection connection) {
                try {
                    AsyncMessageSendAction action = new AsyncMessageSendAction();
                    CommandLine commandLine = new CommandLine(
                            "-diConfig",
                            "nablarch/fw/messaging/action/AsyncMessageSendActionTest.xml",
                            "-userId", "",
                            "-requestPath", "AsyncMessageSendAction/",
                            "-messageRequestId", "MSGREQ0003");
                    action.initialize(commandLine, null);
                    action.transactionNormalEnd(new SqlRow(new HashMap<String, Object>(
                            0), new HashMap<String, Integer>(0),
                            new HashMap<String, String>(0)), new ExecutionContext());
                    fail();
                } catch (IllegalStateException e) {
                    assertThat(e.getMessage(), is(
                            "update data was not single record. updated record count = 0"));
                }
                return null;
            }
        }.doTransaction();
    }

    /** データレコードのフォーマット定義ファイルが存在しない場合。 */
    @Test
    public void formatNotFound() {
        AsyncMessageSendAction action = new AsyncMessageSendAction();
        CommandLine commandLine = new CommandLine(
                "-diConfig", "diConfig",
                "-requestPath", "requestPath",
                "-userId", "userId",
                "-messageRequestId", "messageRequestId"
        );
        AsyncMessageSendActionSettings settings = new AsyncMessageSendActionSettings();
        settings.setFormatDir("format");
        settings.setHeaderFormatName("header");
        repositoryResource.addComponent("asyncMessageSendActionSettings", settings);

        FilePathSetting filePathSetting = new FilePathSetting();
        filePathSetting.addBasePathSetting("format", "file:.");
        filePathSetting.addFileExtensions("format", "format");
        repositoryResource.addComponent("filePathSetting", filePathSetting);

        action.initialize(commandLine, null);
        try {
            action.createDataRecordFormatter();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(allOf(
                    containsString("invalid layout file path was specified."),
                    containsString("file path=["),
                    containsString(new File("./messageRequestId_SEND.format").getAbsolutePath())
            )));
        }
        try {
            action.createHeaderRecordFormatter();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(allOf(
                    containsString("invalid layout file path was specified."),
                    containsString("file path=["),
                    containsString(new File("./header.format").getAbsolutePath())
            )));
        }
    }

    private DataRecordFormatter createFormatter(String formatFileName) {
        formatFileName = formatFileName.startsWith("header")
                ? formatFileName
                : formatFileName + "_SEND";
        FormatterFactory formatterFactory = FormatterFactory.getInstance();
        return formatterFactory.createFormatter(
                FilePathSetting.getInstance()
                               .getFileIfExists("format",
                                       formatFileName));
    }
}

