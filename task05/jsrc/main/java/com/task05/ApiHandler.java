package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
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


import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

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
	private static final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

	@Override
	public APIGatewayV2HTTPResponse handleRequest(Object event, Context context) {
		LambdaLogger logger = context.getLogger();
		try {
			logger.log("Received event: " + event.toString());
			logger.log("Lambda logger EVENT: " + objectMapper.writeValueAsString(event));
			logger.log("EVENT TYPE: " + event.getClass());
			// Parse input from APIGatewayV2
			Map<String, Object> input = objectMapper.readValue(objectMapper.writeValueAsString(event), LinkedHashMap.class);

			// Parse input
			Integer principalId = (Integer) input.get("principalId");
			Map<String, String> content = (Map<String, String>) input.get("content");

			// Generate UUID
			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			// Prepare item for DynamoDB
			Item item = new Item();
			item.withString("id", eventId);
			item.withInt("principalId", principalId);
			item.withString("createdAt", createdAt);
			item.withMap("body", content);


			// Save item to DynamoDB
			PutItemRequest putItemRequest = new PutItemRequest().withTableName(System.getenv("table")).withItem(ItemUtils.toAttributeValues(item));
			PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

			logger.log("putItemResult:" + putItemResult.toString());

			// Prepare response
			Map<String, Object> responseBody = new HashMap<>();
			responseBody.put("id", eventId);
			responseBody.put("principalId", principalId);
			responseBody.put("createdAt", createdAt);
			responseBody.put("body", content);

			APIGatewayV2HTTPResponse response = new APIGatewayV2HTTPResponse();
			response.setStatusCode(201);
			response.setBody(objectMapper.writeValueAsString(Map.of("event", responseBody)));
			response.setHeaders(Map.of("Content-Type", "application/json"));

			return response;
		} catch (Exception e) {
			logger.log("Error in processing request: " + e.getMessage());
			return APIGatewayV2HTTPResponse.builder()
					.withStatusCode(500)
					.withBody("{\"message\": \"Internal server error\"}")
					.withHeaders(Map.of("Content-Type", "application/json"))
					.build();
		}
	}
}
