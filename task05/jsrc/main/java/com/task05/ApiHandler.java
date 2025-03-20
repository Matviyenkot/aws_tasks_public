package com.task05;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;


import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
@DependsOn(name = "Events", resourceType = ResourceType.DYNAMODB_TABLE)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "region", value = "${region}"),
		@EnvironmentVariable(key = "table", value = "${target_table}")})
public class ApiHandler implements RequestHandler<Object, APIGatewayV2HTTPResponse> {
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final DynamoDbClient dynamoDB = DynamoDbClient.create();

	@Override
	public APIGatewayV2HTTPResponse handleRequest(Object event, Context context) {
		LambdaLogger logger = context.getLogger();
		try {
			logger.log("Received event: " + objectMapper.writeValueAsString(event));

			Map<String, Object> data = objectMapper.convertValue(event, LinkedHashMap.class);

			String bodyString = (String) data.get("body");
			if (bodyString == null || bodyString.isEmpty()) {
				return createErrorResponse(400, "Request body is missing");
			}

			Map<String, Object> body = objectMapper.readValue(bodyString, LinkedHashMap.class);

			Object principalIdObj = body.get("principalId");
			if (principalIdObj == null) {
				return createErrorResponse(400, "Missing required field: principalId");
			}
			Integer principalId = (principalIdObj instanceof Number)
					? ((Number) principalIdObj).intValue()
					: Integer.parseInt(principalIdObj.toString());

			Map<String, Object> content = (Map<String, Object>) body.get("content");
			if (content == null) {
				return createErrorResponse(400, "Missing required field: content");
			}

			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			Map<String, AttributeValue> itemMap = new HashMap<>();
			itemMap.put("id", AttributeValue.builder().s(eventId).build());
			itemMap.put("principalId", AttributeValue.builder().n(String.valueOf(principalId)).build());
			itemMap.put("createdAt", AttributeValue.builder().s(createdAt).build());
			itemMap.put("body", AttributeValue.builder().m(
					content.entrySet().stream()
							.collect(Collectors.toMap(
									Map.Entry::getKey,
									e -> AttributeValue.builder().s(String.valueOf(e.getValue())).build()
							))
			).build());

			PutItemRequest putItemRequest = PutItemRequest.builder()
					.tableName(System.getenv("table"))
					.item(itemMap)
					.build();
			dynamoDB.putItem(putItemRequest);

			Map<String, Object> responseBody = new HashMap<>();
			responseBody.put("id", eventId);
			responseBody.put("principalId", principalId);
			responseBody.put("createdAt", createdAt);
			responseBody.put("body", content);

			return createSuccessResponse(201, responseBody);
		} catch (Exception e) {
			logger.log("Error in processing request: " + e.getMessage());
			return createErrorResponse(500, "Internal Server Error");
		}
	}

	private APIGatewayV2HTTPResponse createSuccessResponse(int statusCode, Map<String, Object> body) throws JsonProcessingException {
		return APIGatewayV2HTTPResponse.builder()
				.withStatusCode(statusCode)
				.withBody(objectMapper.writeValueAsString(Map.of("event", body)))
				.withHeaders(Map.of("Content-Type", "application/json"))
				.build();
	}

	private APIGatewayV2HTTPResponse createErrorResponse(int statusCode, String message) {
		return APIGatewayV2HTTPResponse.builder()
				.withStatusCode(statusCode)
				.withBody("{\"error\": \"" + message + "\"}")
				.withHeaders(Map.of("Content-Type", "application/json"))
				.build();
	}
}

