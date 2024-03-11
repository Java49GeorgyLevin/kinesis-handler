package telran.aws.lambda;

import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class KinesisHandler implements RequestHandler<KinesisEvent, String> {

	private static final String TABLE_NAME = "sensor_data";

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		var logger = context.getLogger();
		logger.log("handler started");

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDb = new DynamoDB(client);
		Table table = dynamoDb.getTable(TABLE_NAME);
		try {
			List<Map<String, Object>> records = input.getRecords().stream().map(r -> {
				try {
					return toMap(r);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return null;
			}).toList();
			records.forEach(r -> {
				logger.log(r.toString());
				table.putItem(new PutItemSpec().withItem(Item.fromMap(r)));
			});

		} catch (Exception e) {
			logger.log("error: " + e.toString());
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> toMap(KinesisEventRecord rec) throws ParseException {
		String str = new String(rec.getKinesis().getData().array());
		int index = str.indexOf('{'); // start JSON data
		String jsonStr = str.substring(index);
		JSONParser parser = new JSONParser();
		Map<String, Object> mapJSON = (Map<String, Object>) parser.parse(jsonStr);
		return mapJSON;

	}

}