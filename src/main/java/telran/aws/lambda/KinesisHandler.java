package telran.aws.lambda;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class KinesisHandler implements RequestHandler<KinesisEvent, String> {

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		var logger = context.getLogger();
		logger.log("handler started");
		List<String> records = input.getRecords().stream().map(r -> new String(r.getKinesis().getData().array())).toList();
		records.forEach(logger::log);
		return null;
	}

}
