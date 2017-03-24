INSERT_MESSAGE =
INSERT INTO RECEIVE_MESSAGE_1
  (
  MESSAGE_ID,
  JSN_DATE,
  KEI_NO,
  ITEM_CODE_1,
  ITEM_NAME_1,
  ITEM_AMOUNT_1,
  ITEM_CODE_2,
  ITEM_NAME_2,
  ITEM_AMOUNT_2,
  INSERT_USER_ID,
  INSERT_DATE,
  INSERT_EXECUTION_ID,
  INSERT_REQUEST_ID,
  UPDATED_USER_ID,
  UPDATED_DATE
  ) VALUES (
  :messageId,
  :jsnDate,
  :keiNo,
  :itemCode1,
  :itemName1,
  :itemAmount1,
  :itemCode2,
  :itemName2,
  :itemAmount2,
  :insertUserId,
  :insertDate,
  :insertExecutionId,
  :insertRequestId,
  :updatedUserId,
  :updatedDate
  )