# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import pika
import sys

from solution.consumer_sol import mqConsumer  # pylint: disable=import-error

def main(sector: str, queueName: str) -> None:
    
    # Implement Logic to Create Binding Key from the ticker and sector variable -  Step 2
    #
    #                       WRITE CODE HERE!!!
    #
    

    # Declare the topic exchange
    channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
    routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'
    consumer.channel.basic_publish(
    exchange='topic_logs', routing_key=routing_key, body=message)
    print(f" [x] Sent {routing_key}:{message}")
    
    consumer = mqConsumer(binding_key=sector.bindingKey,exchange_name="Tech Lab Topic Exchange",queue_name=queueName)    
    consumer.startConsuming()
    


if __name__ == "__main__":

    # Implement Logic to read the sector and queueName string from the command line and save them - Step 1
    #
    #                       WRITE CODE HERE!!!
    #
    sector = sys.argv[1]
    queue = sys.argv[2]
    
    sys.exit(main(sector,queue))
