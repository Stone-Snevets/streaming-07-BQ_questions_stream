"""
Program to read in 30-point question information from a queue
-> Queue comes from  `Read-and-Produce-data.py`

Author: Solomon Stevens
Date: June 13, 2024

Basic Steps:
1. Establish a connection to the queue
2. Read in information from the queue
3. Separate the information by introductory remark
   -> NOTE: A combination of introductory remarks will be treated different than one singular introductory remark
4. Calculate the average hit point of each introductory remark (or combo)
5. Write all the averages to a common output file

"""

# ===== Preliminary ===========================================================

# Imports
import pika
import sys

# Constants
HOST = 'localhost'
OUTPUT_FILE = 'Results.txt'
QUEUE_NAME = '30_pt_queue'
POINT_VALUE:int = 30

# Create a lsit for all unique introductory remarks
unique_intro = []


# ===== Functions =============================================================

# Write the results to an output file
def output_to_file():
    """
    Function to take the results we found and write them to a common output file
    This will condense our findings to just one file which makes for easier findings

    NOTE: this will only run when the queue is manually shut down using CTRL + C

    """

    # Open the output file
    #-> Use `a` instead of `w` to append to the output instead of overwrite
    #-> For more information:
    #   https://stackoverflow.com/questions/22441803/how-to-write-to-a-file-without-overwriting-current-contents
    with open(OUTPUT_FILE, 'a') as output_file:

        # Write a title for category of questions
        output_file.write(f'\n{POINT_VALUE}-Point Questions:\n\n')

        # Write a header row
        output_file.write('Introductory Remarks\tCount\tAverage Hit Point\n')

        # For each unique introduction
        for i in range(len(unique_intro)):

            # Write the introduction remarks to the file
            #-> Cast the information to a string to successfully write
            #-> Create a minimun size of this field at 24
            output_file.write(f'{str(sorted(unique_intro)[i][0]):24}')

            # Write the number of occurances to the file
            #-> Cast the information to a string to successfully write
            #-> Create a minimun size of this field at 8
            output_file.write(f'{str(sorted(unique_intro)[i][1]):8}')

            # Write the average hit point to the file
            #-> Cast the information to a string to successfully write
            #-> Round the floating values to 2 decimal places
            output_file.write(f'{sorted(unique_intro)[i][2]:.2f}')

            # Place the next intro in a new line
            output_file.write('\n')
        


# Return a list of just the introductory remarks
def list_intros():
    """
    Function returns all current introductory remarks
    This is used for indexing to see if an intro alredy exists

    For more information:
    https://www.geeksforgeeks.org/python-get-first-element-of-each-sublist/

    """

    return [question_list[0] for question_list in unique_intro]


# Find the correct existing list to work with
def find_existing_list(introductory_tuple, hit_point):
    """
    Function to modify the appropriate list consisting of... 
        The intro tuple
        Number of occurances
        Average hit point so far

    * Increment the number of occurances by one
    * Re-calculate the average hit point

    Parameters:
        introductory_tuple (tuple): The introductory remarks we want to match
        hit_point (float): The hit point of the current question

    """

    # Local variable for the index
    #-> initalize to zero
    existing_index:int = 0

    # Find the correct list
    #-> Check if we have the correct intro
    while introductory_tuple != unique_intro[existing_index][0]:
        #-> If not, increment the index and try again until we match
        existing_index += 1

    # Increment the middle number
    #-> This serves as the number of times that intro occurs
    unique_intro[existing_index][1] += 1

    # Calculate the new average hit point
    #-> old average plus new hit point / incremented count
    unique_intro[existing_index][2] = ((unique_intro[existing_index][2] + hit_point) / unique_intro[existing_index][1])



# Calculate average hit point for each introductory remark
def avg_hit_pt(question):
    """
    Function to calculate the average hit point for listed questions

    Parameters:
        question (str): All the information for the given question

    """

    # Split the string into respective topics
    #-> Set the maximum number of splits to 7.
    #-> There are some notes with a comma in them we want to avoid.
    round_num, question_num, pt_value, q_type, a_type, loc_type, hit_pt, notes = question.split(',', 7)

    # Create a list of the following introductory remarks:
    #-> Question type
    #-> Answer type
    #-> Location type
    intro_tuple = (q_type, a_type, loc_type)

    # Check if the tuple already exists in our `unique_intro` list
    #-> Call `list_intros()` to get a list of all the intros we have so far
    if intro_tuple in list_intros():
        # If it does, call the `find_existing_list` function
        #-> It will find the correct list, increment its counter, and re-calculate its average hit point
        find_existing_list(intro_tuple, float(hit_pt))

    # If this is a new intro combo, append it to the list
    else:
        unique_intro.append([intro_tuple, 1, int(hit_pt)])


# Callback function
def callback(ch, method, properties, body):
    """
    Function to define the behavior on how to receive a message

    Parameters:
        ch: the channel for receiving messages
        method: metadata about delivery
        properties: user-defined properties
        body: the actual message

    Read more about it here:
    https://stackoverflow.com/questions/34202345/rabbitmq-def-callbackch-method-properties-body

    """

    # Send the message to `avg_hit_pt()`
    avg_hit_pt(body.decode())

    # Acknowledge that the message is received and processed
    print(f'Received and processed {body.decode()}')
    ch.basic_ack(delivery_tag = method.delivery_tag)


# Main function
def main(host_name = 'localhost', queue_name = 'default_queue'):
    """
    Create a connection and channel to the queue and receive messages
    Program never ends until stopped by user (CTRL + C)

    Parameters:
        host_name (str): (Default: localhost): the host or IP address
        queue_name (str): (Default: default_queue): the name of the queue to connect to

    """

    # Create a connection
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host = host_name))

    except Exception as e:
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host: {host_name}.")
        print(f"The error says: {e}")
        sys.exit(1)

    # Create a channel and connect it to the queue
    try:
        # Create a channel
        ch = conn.channel()

        # Declare the queue
        #-> Make the queue durable
        #-> Use the channel to do so
        ch.queue_declare(queue = queue_name, durable = True)

        # Limit the number of messages the worker can work with at one time
        ch.basic_qos(prefetch_count=1)

        # Configure the channel to listen to the correct queue
        #-> Let callback handle the acknowledging of messages
        ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        # Start consuming messages
        print('Ready for action! Press CTRL + C to manually close the connection.')
        ch.start_consuming()
    
    except Exception as e:
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)

    # If user manually ends the system
    except KeyboardInterrupt:
        print('User interrupted continuous listening process')

        # Write results to a file
        print('\nWriting results to a file...')
        output_to_file()

        # Acknowledge that the results were written to the file
        print('Results sent successfully.\n')

        # Exit the system
        sys.exit(0)

    # Close the connection
    finally:
        # Close the connection
        print('Closing Connection...')
        conn.close()


# ===== Main ==================================================================
if __name__ == '__main__':
    # Call the main function
    main(HOST, QUEUE_NAME)