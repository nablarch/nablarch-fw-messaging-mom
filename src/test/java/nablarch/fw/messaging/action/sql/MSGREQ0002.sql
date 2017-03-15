INSERT_MESSAGE =
INSERT INTO RECEIVE_MESSAGE_2
  (
  MESSAGE_ID,
  JSN_DATE,
  KANJI_NAME,
  KANA_NAME,
  MAIL_ADDRESS,
  EXTENSION_NUMBER_BUILDING,
  EXTENSION_NUMBER_PERSONAL,
  INSERT_DATE,
  INSERT_USER_ID,
  INSERT_EXECUTION_ID,
  INSERT_REQUEST_ID,
  UPDATED_DATE,
  UPDATED_USER_ID
  ) VALUES (
  :messageId,
  :jsnDate,
  :kanjiName,
  :kanaName,
  :mailAddress,
  :extensionNumberBuilding,
  :extensionNumberPersonal,
  :insertDate,
  :insertUserId,
  :insertExecutionId,
  :insertRequestId,
  :updatedDate,
  :updatedUserId
  )
