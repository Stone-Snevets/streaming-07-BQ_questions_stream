"""
Program to read in data from CSV files
Data will be sent to different queues

Author: Solomon Stevens
Date: June 13, 2024

Basic Steps:
1. Read in contents of CSV file
2. Create a connection to RabbitMQ
   -> We are using RabbitMQ to create queues
3. Send contents to respective queues

"""

# ===== Preliminary ===========================================================

# Imports
import csv
import pika
import sys

# Constants
DEFAULT_DELIM = '\t'
HOST = 'localhost'
INPUT_FILE = 'SourceFiles/DRN_Nats.csv'
QUEUE_10S = '10_pt_queue'
QUEUE_20S = '20_pt_queue'
QUEUE_30S = '30_pt_queue'


# ===== Functions =============================================================

# Read data from the CSV file and send it to queues
def send_msg(host:str,
             queue_name_1:str,
             queue_name_2:str,
             queue_name_3:str,
             file_name:str):
    """
    Function to send messages to the queue
    -> This process runs and finishes

    Parameters:
        host (str): the hostname or IP address
        queue_name_1 (str): the name of the first queue
        queue_name_2 (str): the name of the second queue
        queue_name_3 (str): the name of the third queue
        file_name (str): the name of the file to open

    """

    # Acknowledge the function opened
    print(f'Opened `send_msg({host}, {queue_name_1}, {queue_name_2}, {queue_name_3}, {file_name})`')

    # Open the file
    with open(file_name, 'r') as input_file:

        # Create a reader object
        reader  = csv.reader(input_file, delimiter=DEFAULT_DELIM)

        # Remove the header row
        header = next(reader)

        # --- Establish a connection to RabbitMQ ---
        try:
            # Create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))

            # Use the connection to create a communication channel
            ch = conn.channel()

            # Delete existing queues
            #-> This removes whatever contents may be left from a
            #   previous declaration of the queue.
            ch.queue_delete(queue=queue_name_1)
            ch.queue_delete(queue=queue_name_2)
            ch.queue_delete(queue=queue_name_3)

            # Use the channel to declare durable queues
            #-> A durable queue will survive a RabbitMQ server restart
            #   and help ensure messages are processed in order.
            #-> Messages will not be deleted until the consumer acknowledges.
            ch.queue_declare(queue=queue_name_1, durable=True)
            ch.queue_declare(queue=queue_name_2, durable=True)
            ch.queue_declare(queue=queue_name_3, durable=True)

            # For each row in the file
            for row in reader:
                # Check to make sure the row isn't a newline
                if row != []:

                    # If not, assign each part of the row to an element
                    round_num, question_num, pt_value, q_type, a_type, loc_type, hit_pt, notes = row

                    # Create a format string of the row's contents
                    f_str = f'[{round_num}, {question_num}, {pt_value}, {q_type}, {a_type}, {loc_type}, {hit_pt}, {notes}]'

                    # Generate a binary message of our f string
                    msg = f_str.encode()

                    # Send row to respective queue
                    #-> Queue is based on point value
                    if pt_value == '10':
                        ch.basic_publish(exchange="", routing_key=queue_name_1, body=msg)
                        print(f'Sending {row} to {queue_name_1}')
                        
                    if pt_value == '20':
                        ch.basic_publish(exchange="", routing_key=queue_name_2, body=msg)
                        print(f'Sending {row} to {queue_name_2}')

                    if pt_value == '30':
                        ch.basic_publish(exchange="", routing_key=queue_name_3, body=msg)
                        print(f'Sending {row} to {queue_name_3}')


        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error: Connection to RabbitMQ server failed: {e}")
            sys.exit(1)

        finally:
            # close the connection to the server
            conn.close()


# ===== Main ==================================================================
if __name__ == "__main__":

    # Send the file to the queue
    send_msg(HOST, QUEUE_10S, QUEUE_20S, QUEUE_30S, INPUT_FILE)