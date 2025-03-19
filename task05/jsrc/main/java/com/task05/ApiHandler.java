package com.task05;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "table_name", value = "${target_table}"),
		@EnvironmentVariable(key = "region", value = "${region}")}
)
public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	private static final String TABLE_NAME = System.getenv("table_name");
	private static final String REGION = System.getenv("region");

	private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
			.region(Region.of(REGION))
			.credentialsProvider(DefaultCredentialsProvider.create())
			.build();

	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		try {
			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			int principalId = (int) request.get("principalId");
			Map<String, String> content = (Map<String, String>) request.get("content");

			Map<String, AttributeValue> item = new HashMap<>();
			item.put("id", AttributeValue.builder().s(eventId).build());
			item.put("principalId", AttributeValue.builder().n(String.valueOf(principalId)).build());
			item.put("createdAt", AttributeValue.builder().s(createdAt).build());
			item.put("body", AttributeValue.builder().m(convertToAttributeMap(content)).build());

			PutItemRequest putItemRequest = PutItemRequest.builder()
					.tableName(TABLE_NAME)
					.item(item)
					.build();
			dynamoDbClient.putItem(putItemRequest);

			Map<String, Object> responseEvent = new HashMap<>();
			responseEvent.put("id", eventId);
			responseEvent.put("principalId", principalId);
			responseEvent.put("createdAt", createdAt);
			responseEvent.put("body", content);

			Map<String, Object> response = new HashMap<>();
			response.put("statusCode", 201);
			response.put("event", responseEvent);
			return response;
		} catch (Exception e) {
			context.getLogger().log("Error: " + e.getMessage());
			Map<String, Object> errorResponse = new HashMap<>();
			errorResponse.put("statusCode", 500);
			errorResponse.put("message", "Internal Server Error");
			return errorResponse;
		}
	}

	private Map<String, AttributeValue> convertToAttributeMap(Map<String, String> map) {
		Map<String, AttributeValue> attributeMap = new HashMap<>();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			attributeMap.put(entry.getKey(), AttributeValue.builder().s(entry.getValue()).build());
		}
		return attributeMap;
	}
}
