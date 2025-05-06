from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

payload = {
	"schema": {
		"type": "struct",
		"fields": [
			{
				"field": "username",
				"type": "string",
                "optional": False
			},
			{
				"field": "email",
				"type": "string",
                "optional": False
			}
		],
        "optional": False,
        "version": 1
	},
	"payload": {
		"username": "testuser3",
		"email": "testuser@example.com"
	}
}

key = {
  "schema": {
    "type": "struct",
    "fields": [
      {
        "field": "id",
        "type": "string",
        "optional": False
      }
    ],
    "optional": False,
    "version": 1
  },
  "payload": {
    "id": "user123"
  }
}



# Example to add to table
producer.send('users', key=json.dumps(key).encode('utf-8'), value = json.dumps(payload).encode('utf-8'))

# Example to delete from table
producer.send('users', key=json.dumps(key).encode('utf-8'), value = None)

producer.flush()