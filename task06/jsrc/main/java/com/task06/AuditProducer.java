package com.task06;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;


import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}")})
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {

	private static final DynamoDbClient dynamoDB = DynamoDbClient.create();
	private static final String AUDIT_TABLE_NAME = System.getenv("table");

	@Override
	public Void handleRequest(DynamodbEvent event, Context context) {
		for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
			StreamRecord streamRecord = record.getDynamodb();
			if (streamRecord == null || streamRecord.getKeys() == null) {
				continue;
			}

			Map<String, AttributeValue> keys = streamRecord.getKeys();
			Map<String, AttributeValue> newImage = streamRecord.getNewImage();
			Map<String, AttributeValue> oldImage = streamRecord.getOldImage();

			if (!keys.containsKey("key")) {
				continue;
			}

			String itemKey = keys.get("key").getS();
			String modificationTime = Instant.now().toString();
			String eventId = UUID.randomUUID().toString();

			Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> auditItem = new HashMap<>();
			auditItem.put("id", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(eventId).build());
			auditItem.put("itemKey", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(itemKey).build());
			auditItem.put("modificationTime", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(modificationTime).build());

			if ("INSERT".equals(record.getEventName()) && newImage != null) {
				Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> newValueMap = new HashMap<>();
				newValueMap.put("key", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(itemKey).build());
				newValueMap.put("value", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(newImage.get("value").getN()).build());

				auditItem.put("newValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().m(newValueMap).build());

			} else if ("MODIFY".equals(record.getEventName()) && newImage != null && oldImage != null) {
				auditItem.put("newValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
						.n(newImage.get("value").getN())
						.build());
				auditItem.put("oldValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
						.n(oldImage.get("value").getN())
						.build());
			}

			PutItemRequest putItemRequest = PutItemRequest.builder()
					.tableName(AUDIT_TABLE_NAME)
					.item(auditItem)
					.build();

			dynamoDB.putItem(putItemRequest);
		}
		return null;
	}

	private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> convertToSdkMap(Map<String, AttributeValue> legacyMap) {
		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> sdkMap = new HashMap<>();
		for (Map.Entry<String, AttributeValue> entry : legacyMap.entrySet()) {
			AttributeValue legacyValue = entry.getValue();
			software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder builder = software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder();
			if (legacyValue.getS() != null) builder.s(legacyValue.getS());
			if (legacyValue.getN() != null) builder.n(legacyValue.getN());
			if (legacyValue.getM() != null) builder.m(convertToSdkMap(legacyValue.getM()));
			sdkMap.put(entry.getKey(), builder.build());
		}
		return sdkMap;
	}
}

