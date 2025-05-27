import json

topics = ['concurrentPub/topic/' + str(i) for i in range(1, 10001)]
event = {'topics': topics}

with open('test_event.json', 'w') as f:
    json.dump(event, f, indent=2)

print(f"Generated test_event.json with {len(topics)} topics")
