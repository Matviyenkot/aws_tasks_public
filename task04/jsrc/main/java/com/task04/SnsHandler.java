package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.events.SnsEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
		lambdaName = "sns_handler",
		roleName = "sns_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@SnsEventSource(
		targetTopic = "lambda_topic"
)
public class SnsHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	@Override
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		context.getLogger().log("SNS event received: " + request);
		if (request.containsKey("Records")) {
			Object recordsObj = request.get("Records");
			if (recordsObj instanceof Iterable) {
				for (Object record : (Iterable<?>) recordsObj) {
					if (record instanceof Map) {
						Map<?, ?> recordMap = (Map<?, ?>) record;
						Object snsObj = recordMap.get("Sns");
						if (snsObj instanceof Map) {
							Map<?, ?> snsMap = (Map<?, ?>) snsObj;
							String message = (String) snsMap.get("Message");
							context.getLogger().log("SNS Message: " + message);
						}
					}
				}
			}
		}
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put("statusCode", 200);
		resultMap.put("message", "Message processed");
		return resultMap;
	}
}
