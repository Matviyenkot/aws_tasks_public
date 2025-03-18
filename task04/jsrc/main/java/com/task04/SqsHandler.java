package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.List;
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
public class SqsHandler implements RequestHandler<List<Map<String, Object>>, Map<String, Object>> {

	@Override
	public Map<String, Object> handleRequest(List<Map<String, Object>> messages, Context context) {
		Map<String, Object> resultMap = new HashMap<>();

		for (Map<String, Object> message : messages) {
			context.getLogger().log("Received SQS message: " + message.get("body"));
		}

		resultMap.put("statusCode", 200);
		resultMap.put("body", "Processed " + messages.size() + " messages.");
		return resultMap;
	}
}
