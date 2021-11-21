from kafka import KafkaProducer
from kafka.errors import KafkaError
from pyjavaproperties import Properties
import argparse, sys, logging

from turkey_api_pb2 import (TemperatureChangeCommand)
from turkey_eventsourced_entity import temperature_changed


class KafkaMessageGenerator:
    def __init__(self):
        p = Properties()
        p.load(open('./conf/kafka.properties'))
        self.producer = KafkaProducer(bootstrap_servers=[p['bootstrap.servers.local']])

    def publish(self, topic, command):
        headers = [("ce-type",b"com.example.TemperatureChangeCommand"),("ce-specversion",b"1.0"),("ce-datacontenttype",b"application/protobuf")]
        future = self.producer.send(topic, value=command.SerializeToString(), headers=headers)

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            logging.error(e)
                
def main() -> int:
    parser = argparse.ArgumentParser(description='Kafka Message Generator information')

    parser.add_argument('--id', dest='id', type=str, help='Id of the turkey')
    parser.add_argument('--amount', dest='amount', type=float, help='Temp increase')
    feature_parser = parser.add_mutually_exclusive_group(required=False)
    feature_parser.add_argument('--increase', dest='increase', action='store_true')
    feature_parser.add_argument('--decrease', dest='increase', action='store_false')
    parser.set_defaults(feature=True)
    args = parser.parse_args()
    
    kmg = KafkaMessageGenerator()
    temperature_changed_command = TemperatureChangeCommand(turkey_id=(args.id).encode('utf-8'), temperature_change=(args.amount))
    topic = 'increase_temp' if bool(args.increase) == True else 'decrease_temp'
    kmg.publish(topic, temperature_changed_command)

if __name__ == '__main__':
    sys.exit(main()) 

