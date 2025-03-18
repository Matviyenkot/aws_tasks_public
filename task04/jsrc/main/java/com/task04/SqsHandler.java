package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
		lambdaName = "sqs_handler",
		roleName = "sqs_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED,
		timeout = 350
)
@SqsTriggerEventSource(
		targetQueue = "async_queue",
		batchSize = 10
)
public class SqsHandler implements RequestHandler<SQSEvent, Map<String, Object>> {

	@Override
	public Map<String, Object> handleRequest(SQSEvent event, Context context) {
		Map<String, Object> resultMap = new HashMap<>();
		if (event.getRecords().isEmpty()) {
			context.getLogger().log("No messages received.");
			resultMap.put("statusCode", 204);
			resultMap.put("body", "No messages to process.");
			return resultMap;
		}
		for (SQSEvent.SQSMessage message : event.getRecords()) {
			context.getLogger().log("Received SQS message: " + message.getBody());
		}
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Processed " + event.getRecords().size() + " messages.");
		return resultMap;
	}
}
