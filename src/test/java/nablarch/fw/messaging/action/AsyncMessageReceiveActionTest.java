package nablarch.fw.messaging.action;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import nablarch.common.idgenerator.IdGenerator;
import nablarch.common.idgenerator.formatter.LpadFormatter;
import nablarch.core.dataformat.DataRecordFormatter;
import nablarch.core.dataformat.FormatterFactory;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.FilePathSetting;
import nablarch.fw.ExecutionContext;
import nablarch.fw.launcher.CommandLine;
import nablarch.fw.launcher.Main;
import nablarch.fw.messaging.FwHeader;
import nablarch.fw.messaging.MessageReadError;
import nablarch.fw.messaging.MessagingContext;
import nablarch.fw.messaging.MessagingProvider;
import nablarch.fw.messaging.ReceivedMessage;
import nablarch.fw.messaging.RequestMessage;
import nablarch.fw.messaging.SendingMessage;
import nablarch.fw.messaging.action.form.INVALID_CONSTRUCTORForm;
import nablarch.test.core.messaging.EmbeddedMessagingProvider;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link AsyncMessageReceiveAction}のテスト。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class AsyncMessageReceiveActionTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/fw/messaging/action/AsyncMessageReceiveActionTest.xml");

    /** {@link MessagingContext} */
    private static MessagingContext context = null;

    /**
     * 本テストクラスのセットアップ処理
     * <p/>
     * テストで使用するテーブルの構築を行う。
     */
    @BeforeClass
    public static void classSetUp() {
        VariousDbTestHelper.createTable(MessagingBatchRequest.class);
        VariousDbTestHelper.createTable(MessagingIdGenerate.class);
        VariousDbTestHelper.createTable(ReceiveMessage1.class);
        VariousDbTestHelper.createTable(ReceiveMessage2.class);
    }

    /** 本テストクラスの終了処理 */
    @AfterClass
    public static void classTearDown() {
        EmbeddedMessagingProvider.stopServer();
    }

    /**
     * テストケース単位の事前準備処理。
     * <p/>
     */
    @Before
    public void setUp() {
        VariousDbTestHelper.setUpTable(
                new MessagingIdGenerate("01", 0L),
                new MessagingIdGenerate("02", 0L),
                new MessagingIdGenerate("03", 0L));

        if (context != null) {
            context.close();
        }
    }

    /**
     * テストケース1。
     * <p/>
     * 以下のテストを実施する。
     */
    @Test
    public void testReceiveMessage1() throws Exception {

        // 受信テーブルの準備(レコード削除)
        clearMessageTable();

        //**********************************************************************
        // 受信スレッド(テストターゲットのアクション)の起動
        //**********************************************************************
        final AsyncMessageReceiveActionSettings settings = new AsyncMessageReceiveActionSettings();
        settings.setFormClassPackage("nablarch.fw.messaging.action.form");
        settings.setReceivedSequenceGenerator(createIdGenerate());
        settings.setTargetGenerateId("01");
        settings.setSqlFilePackage(
                "nablarch.fw.messaging.action.sql");
        settings.setReceivedSequenceFormatter(new LpadFormatter(20, '0'));

        repositoryResource.addComponent("asyncMessageReceiveActionSettings", settings);

        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID
        CompletionService<Integer> service =
                executeReceiveThread(requestId, userId);

        //**********************************************************************
        // リクエストID:MSGREQ0001を投入
        // MSGREQ0001のデータは、RECEIVE_MESSAGE_1テーブルにINSERTされる。
        //**********************************************************************
        SendingMessage message1 = new SendingMessage();
        // ヘッダ部
        Map<String, Object> header = new HashMap<String, Object>();
        header.put("requestId", "MSGREQ0001");
        message1.setFormatter(createFormatter("header"));
        message1.addRecord(header);

        // 業務電文部
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("keiNo", "0000000001");
        data.put("itemCode1", "0001");
        data.put("itemName1", "アイテム１");
        data.put("itemAmount1", 100);
        message1.setFormatter(createFormatter("MSGREQ0001"));
        message1.addRecord(data);

        // テスト用の電文送信
        message1.setDestination("RECEIVE.TEST");
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");
        context = messagingProvider.createContext();
        context.send(message1);

        Thread.sleep(3000);

        //**********************************************************************
        // リクエストID:MSGREQ0002を投入
        // MSGREQ0002のデータは、RECEIVE_MESSAGE_2テーブルにINSERTされる。
        // この電文では、Formクラスのサフィックスを「Form２」に入れ替える
        //**********************************************************************
        settings.setFormClassSuffix("Form2");
        settings.setDbTransactionName(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        SendingMessage message2 = new SendingMessage();
        //// ヘッダ部
        header.put("requestId", "MSGREQ0002");
        message2.setFormatter(createFormatter("header"));
        message2.addRecord(header);

        // 電文部
        data.clear();
        data.put("kanjiName", "漢字名称");
        data.put("kanaName", "カナメイショウ");
        data.put("mailAddress", "mail@mail.com");
        data.put("extensionNumberBuilding", "02");
        data.put("extensionNumberPersonal", "1234");
        message2.setFormatter(createFormatter("MSGREQ0002"));
        message2.addRecord(data);
        message2.setDestination("RECEIVE.TEST");
        context.send(message2);

        //**********************************************************************
        // 受信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);
        Future<Integer> take = service.take();
        Integer result = take.get();
        assertThat(result, is(0));

        //**********************************************************************
        // リクエストID:MSGREQ0001のデータをアサート
        // RECEIVE_MESSAGE_1テーブルに登録されているはずなので、そのデータをアサートする。
        //**********************************************************************
        // 受信テーブルのデータを取得
        List<ReceiveMessage1> messageList1 = VariousDbTestHelper.findAll(ReceiveMessage1.class);

        assertThat("メッセージテーブルに1レコード登録されていること", messageList1.size(), is(1));
        assertThat(messageList1.get(0).messageId, is("00000000000000000001"));
        assertThat(messageList1.get(0).keiNo, is("0000000001"));
        assertThat(messageList1.get(0).itemCode1, is("0001"));
        assertThat(messageList1.get(0).itemName1, is("アイテム１"));
        assertThat(messageList1.get(0).itemAmount1, is(100L));
        assertThat(messageList1.get(0).itemCode2, is(nullValue()));
        assertThat(messageList1.get(0).itemName2, is(nullValue()));
        assertThat(messageList1.get(0).itemAmount2, is(0L));

        //**********************************************************************
        // リクエストID:MSGREQ0001のデータをアサート
        // RECEIVE_MESSAGE_1テーブルに登録されているはずなので、そのデータをアサートする。
        //**********************************************************************
        // 受信テーブルのデータを取得
        List<ReceiveMessage2> messageList2 = VariousDbTestHelper.findAll(ReceiveMessage2.class);

        assertThat("メッセージテーブルに1レコード登録されていること", messageList2.size(), is(1));
        assertThat(messageList2.get(0).kanjiName, is("漢字名称"));
        assertThat(messageList2.get(0).kanaName, is("カナメイショウ"));
        assertThat(messageList2.get(0).mailAddress, is("mail@mail.com"));
        assertThat(messageList2.get(0).extensionNumberBuilding, is("02"));
        assertThat(messageList2.get(0).extensionNumberPersonal, is("1234"));
    }

    /**
     * テストケース2。
     * <p/>
     * サロゲートペア対応のテストを実施する。
     */
    @Test
    public void testReceiveMessage2() throws Exception {

        // 受信テーブルの準備(レコード削除)
        clearMessageTable();

        //**********************************************************************
        // 受信スレッド(テストターゲットのアクション)の起動
        //**********************************************************************
        final AsyncMessageReceiveActionSettings settings = new AsyncMessageReceiveActionSettings();
        settings.setFormClassPackage("nablarch.fw.messaging.action.form");
        settings.setReceivedSequenceGenerator(createIdGenerate());
        settings.setTargetGenerateId("01");
        settings.setSqlFilePackage(
                "nablarch.fw.messaging.action.sql");
        settings.setReceivedSequenceFormatter(new LpadFormatter(20, '0'));

        repositoryResource.addComponent("asyncMessageReceiveActionSettings", settings);

        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID
        CompletionService<Integer> service =
                executeReceiveThread(requestId, userId);

        //**********************************************************************
        // リクエストID:MSGREQ0001を投入
        // MSGREQ0001のデータは、RECEIVE_MESSAGE_1テーブルにINSERTされる。
        //**********************************************************************
        SendingMessage message1 = new SendingMessage();
        // ヘッダ部
        Map<String, Object> header = new HashMap<String, Object>();
        header.put("requestId", "MSGREQ0001");
        message1.setFormatter(createFormatter("header"));
        message1.addRecord(header);

        // 業務電文部
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("keiNo", "0000000001");
        data.put("itemCode1", "0001");
        data.put("itemName1", "𪛊𪛊🙀🙀");
        data.put("itemAmount1", 100);
        message1.setFormatter(createFormatter("MSGREQ0001"));
        message1.addRecord(data);

        // テスト用の電文送信
        message1.setDestination("RECEIVE.TEST");
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");
        context = messagingProvider.createContext();
        context.send(message1);

        Thread.sleep(3000);

        //**********************************************************************
        // 受信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);
        Future<Integer> take = service.take();
        Integer result = take.get();
        assertThat(result, is(0));

        //**********************************************************************
        // リクエストID:MSGREQ0001のデータをアサート
        // RECEIVE_MESSAGE_1テーブルに登録されているはずなので、そのデータをアサートする。
        //**********************************************************************
        // 受信テーブルのデータを取得
        List<ReceiveMessage1> messageList1 = VariousDbTestHelper.findAll(ReceiveMessage1.class);

        assertThat("メッセージテーブルに1レコード登録されていること", messageList1.size(), is(1));
        assertThat(messageList1.get(0).messageId, is("00000000000000000001"));
        assertThat(messageList1.get(0).keiNo, is("0000000001"));
        assertThat(messageList1.get(0).itemCode1, is("0001"));
        assertThat(messageList1.get(0).itemName1, is("𪛊𪛊🙀🙀"));
        assertThat(messageList1.get(0).itemAmount1, is(100L));
        assertThat(messageList1.get(0).itemCode2, is(nullValue()));
        assertThat(messageList1.get(0).itemName2, is(nullValue()));
        assertThat(messageList1.get(0).itemAmount2, is(0L));
    }

    /**
     * サービス開閉局制御のテスト
     */
    @Test
    public void testTemporarilyServiceStop() throws Exception {

        //**********************************************************************
        // 受信スレッド(テストターゲットのアクション)の起動
        //**********************************************************************
        final AsyncMessageReceiveActionSettings settings = new AsyncMessageReceiveActionSettings();
        settings.setFormClassPackage("nablarch.fw.messaging.action.form");
        settings.setReceivedSequenceGenerator(createIdGenerate());
        settings.setTargetGenerateId("01");
        settings.setSqlFilePackage(
                "nablarch.fw.messaging.action.sql");
        settings.setReceivedSequenceFormatter(new LpadFormatter(20, '0'));

        repositoryResource.addComponent("asyncMessageReceiveActionSettings", settings);

        //  要求電文を送信する。
        // --> 閉局中のため、サーバ側のキューに滞留したまま処理されない。
        SendingMessage message1 = new SendingMessage();
        // ヘッダ部
        Map<String, Object> header = new HashMap<String, Object>();
        header.put("requestId", "MSGREQ0001");
        message1.setFormatter(createFormatter("header"));
        message1.addRecord(header);

        // 業務電文部
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("keiNo", "0000000001");
        data.put("itemCode1", "0001");
        data.put("itemName1", "アイテム１");
        data.put("itemAmount1", 100);
        message1.setFormatter(createFormatter("MSGREQ0001"));
        message1.addRecord(data);

        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID
        CompletionService<Integer> service =
                executeReceiveThread(requestId, userId);

        Thread.sleep(2000);

        // 受信テーブルの準備(レコード削除)
        clearMessageTable();
        List<ReceiveMessage1> messageList1 = VariousDbTestHelper.findAll(ReceiveMessage1.class);
        assertEquals(0, messageList1.size());

        //------------------ 閉局フラグを設定する。---------------------------// 
        MessagingBatchRequest entity = VariousDbTestHelper.findById(MessagingBatchRequest.class, "R000000001");
        entity.serviceAvailable = "0";
        VariousDbTestHelper.update(entity);

        Thread.sleep(5000); //各スレッドが受信キューでのブロックから外れるのを待つ。

        // テスト用の電文送信
        message1.setDestination("RECEIVE.TEST");
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");

        context = messagingProvider.createContext();
        context.send(message1);

        Thread.sleep(5000); // 送信電文の処理待ち

        // 受信テーブルのデータを取得
        // --> キュー上で要求電文が滞留しているので、未登録。
        messageList1 = VariousDbTestHelper.findAll(ReceiveMessage1.class);

        // 開局する。
        // --> これにより、滞留していた要求電文が処理される。
        entity = VariousDbTestHelper.findById(MessagingBatchRequest.class, "R000000001");
        entity.serviceAvailable = "1";
        VariousDbTestHelper.update(entity);

        Thread.sleep(2000);

        // 受信テーブルのデータを取得
        // --> 滞留電文が処理されたので、正常に登録される。
        messageList1 = VariousDbTestHelper.findAll(ReceiveMessage1.class);
        assertThat("メッセージテーブルに1レコード登録されていること", messageList1.size(), is(1));
        assertThat(messageList1.get(0).keiNo, is("0000000001"));
        assertThat(messageList1.get(0).itemCode1, is("0001"));

        //**********************************************************************
        // 受信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);
        Future<Integer> take = service.take();
        Integer result = take.get();
        assertThat(result, is(0));
    }

    /**
     * ヘッダーフォーマットが不正な場合のテスト。
     * データリーダから MessageReadError が送出され、その内容がFatalログに出力
     * されるが、プロセスは継続する。
     * <p/>
     * 現時点ではAUTO_AQUIREモードで動作しており、
     * POISONキュー制御ができないので、FATALログの出力のみを確認する。
     */
    @Test
    public void testInvalidHeaderFormat() throws Exception {
        //**********************************************************************
        // 受信スレッド(テストターゲットのアクション)の起動
        //**********************************************************************
        final AsyncMessageReceiveActionSettings settings = new AsyncMessageReceiveActionSettings();
        settings.setFormClassPackage("nablarch.fw.messaging.action.form");
        settings.setReceivedSequenceGenerator(createIdGenerate());
        settings.setTargetGenerateId("01");
        settings.setReceivedSequenceFormatter(new LpadFormatter(20, '0'));

        repositoryResource.addComponent("asyncMessageReceiveActionSettings", settings);

        String requestId = "R000000001";        // プロセスのリクエストID
        String userId = "batchUser1";           // バッチ起動時のユーザID

        //**********************************************************************
        // リクエストID:MSGREQ0001を投入
        // MSGREQ0001のデータは、RECEIVE_MESSAGE_1テーブルにINSERTされる。
        //**********************************************************************
        SendingMessage message1 = new SendingMessage();
        // ヘッダ部
        Map<String, Object> header = new HashMap<String, Object>();
        header.put("requestId", "1");
        message1.setFormatter(createFormatter("invalid-header"));
        message1.addRecord(header);

        // テスト用の電文送信
        message1.setDestination("RECEIVE.TEST");
        MessagingProvider messagingProvider = repositoryResource.getComponent("messagingProvider");
        MessagingContext messagingContext = messagingProvider.createContext();

        CompletionService<Integer> service = executeReceiveThread(requestId, userId);
        messagingContext.send(message1);

        //**********************************************************************
        // 受信スレッドを停止する。
        //**********************************************************************
        stopProcess(requestId);
        Future<Integer> take = service.take();
        Integer result = take.get();
        assertThat(result, is(0));

        Throwable e = ExceptionCatcher.getThrown();
        assertEquals(MessageReadError.class, e.getClass());


    }

    /**
     * リクエストIDに対応するフォーマットクラスが存在しない場合。
     */
    @Test
    public void testFormClassNotFound() {

        AsyncMessageReceiveActionSettings settings = new AsyncMessageReceiveActionSettings();
        settings.setFormClassPackage("nablarch.fw.messaging.action.form");
        repositoryResource.addComponent("asyncMessageReceiveActionSettings", settings);

        AsyncMessageReceiveAction action = new AsyncMessageReceiveAction();
        FwHeader header = new FwHeader();

        header.setRequestId("HOGE");
        RequestMessage message = new RequestMessage(header, new ReceivedMessage(new byte[0]));
        message.setFormatter(createFormatter("MSGREQ0001"));

        ExecutionContext context = new ExecutionContext();
        try {
            action.handle(message, context);
            fail("does not run.");
        } catch (RuntimeException e) {
            // nablarch.fw.messaging.action.form.HOGEFormは存在しないためエラーとなる。
            assertThat(e.getMessage(), containsString(
                    "form class was not found. form class name = nablarch.fw.messaging.action.form.HOGEForm"));
        }

    }

    /**
     * String, RequestMessageのコンストラクタを持たないFormの場合。
     */
    @Test
    public void testInvalidFormClassConstructor() {

        AsyncMessageReceiveActionSettings settings = new AsyncMessageReceiveActionSettings();
        settings.setFormClassPackage("nablarch.fw.messaging.action.form");
        repositoryResource.addComponent("asyncMessageReceiveActionSettings", settings);

        AsyncMessageReceiveAction action = new AsyncMessageReceiveAction();
        FwHeader header = new FwHeader();

        header.setRequestId("INVALID_CONSTRUCTOR");
        RequestMessage message = new RequestMessage(header, new ReceivedMessage(new byte[0]));
        message.setFormatter(createFormatter("MSGREQ0001"));

        ExecutionContext context = new ExecutionContext();
        try {
            action.handle(message, context);
            fail("does not run.");
        } catch (RuntimeException e) {
            // nablarch.fw.messaging.action.form.INVALID_CONSTRUCTORFormクラスには、
            // String, RequestMessageのコンストラクタが存在しない。
            assertThat(e.getMessage(), is(
                    "required constructor was not found. required constructor:first parameter is String, second parameter is RequestMessage. form class name = "
                            + INVALID_CONSTRUCTORForm.class.getName()));
        }

    }

    /** メッセージテーブルの状態を0レコードにする。 */
    private static void clearMessageTable() {
        VariousDbTestHelper.delete(ReceiveMessage1.class);
        VariousDbTestHelper.delete(ReceiveMessage2.class);
    }

    /**
     * 採番用のクラスを生成する。
     *
     * @return 採番クラス
     */
    private IdGenerator createIdGenerate() {
        MockTableIdGenerator generator = new MockTableIdGenerator();
        generator.setTableName("MESSAGING_ID_GENERATE");
        generator.setIdColumnName("ID");
        generator.setNoColumnName("NO");
        generator.initialize();
        return generator;
    }

    /** ヘッダファイル用フォーマッタを生成する。 */
    private DataRecordFormatter createFormatter(String formatFileName) {
        formatFileName = formatFileName.endsWith("header")
                ? formatFileName
                : formatFileName + "_RECEIVE";
        FormatterFactory formatterFactory = FormatterFactory.getInstance();
        return formatterFactory.createFormatter(
                FilePathSetting.getInstance()
                        .getFileIfExists("format",
                                formatFileName));
    }

    /**
     * プロセスを停止する。
     *
     * @param requestId リクエストID
     */
    private static void stopProcess(
            final String requestId) throws InterruptedException {

        Thread.sleep(3000);

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
     * テスト用の電文受信プロセスを実行する。
     *
     * @param requestId リクエストID
     * @param userId ユーザID
     */
    private static CompletionService<Integer> executeReceiveThread(
            final String requestId, final String userId) throws InterruptedException {

        VariousDbTestHelper.setUpTable(
                new MessagingBatchRequest("R000000001", "リクエスト０１", "0", "0", "1"),
                new MessagingBatchRequest("R000000002", "リクエスト０２", "0", "0", "1"),
                new MessagingBatchRequest("R000000003", "リクエスト０３", "0", "0", "1"),
                new MessagingBatchRequest("R000000004", "リクエスト０４", "0", "0", "1"));

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        CompletionService<Integer> service =
                new ExecutorCompletionService<Integer>(executorService);

        service.submit(new Callable<Integer>() {
            public Integer call() throws Exception {
                CommandLine commandLine = new CommandLine(
                        "-diConfig",
                        "nablarch/fw/messaging/action/AsyncMessageReceiveActionTest.xml",
                        "-userId", userId,
                        "-requestPath",
                        "AsyncMessageReceiveAction/" + requestId);
                return Main.execute(commandLine);
            }
        });

        // プロセスアクティブフラグがONになるまで待機
        int count = 0;
        while (true) {
            MessagingBatchRequest query = VariousDbTestHelper.findById(MessagingBatchRequest.class, requestId);
            if (query == null) {
                break;
            }

            if ("1".equals(query.processActiveFlg)) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (count++ > 100) {
                throw new RuntimeException(
                        "テスト失敗！！！テスト用の電文受信プロセスが起動に失敗しました。");
            }
        }
        return service;
    }
}
