import argparse
import csv

def read_csv_lines(filename):
    try:
        with open(filename, "r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            return [row for row in reader]  # Returns list of lists
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
    except PermissionError:
        print(f"Error: Permission denied for file '{filename}'.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return []


def write_lines_to_file(script_filepath, method_name, output_dir_path, clients, sleep_secs):
    start_uid = 1 # hardcoded for small clients 
    for client, end_uid in clients:
        line = f"""sh {script_filepath} {client} {method_name} {start_uid} {end_uid};
sleep({sleep_secs});     
"""
        output_filepath = f'{output_dir_path}/{method_name}_small_client_migration_commands.sh' 
        with open(output_filepath, "a") as file:
            file.write(line)

def create_migration_commands(script_filepath, output_dir_path, producer_methods, consumer_methods, clients, sleep_secs):
    for producer_method in producer_methods:
        write_lines_to_file(script_filepath, producer_method, output_dir_path, clients, sleep_secs)

    for consumer_method in consumer_methods:
        write_lines_to_file(script_filepath, consumer_method, output_dir_path, clients, sleep_secs)
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to read panels names and create migration commands.")
    parser.add_argument("--inputfilename", help="Path to the panel names CSV file.", default="panel_names.csv")
    parser.add_argument("--outputdirpath", help="Path to the output migration commands file.", default="migration_commands")
    parser.add_argument("--sleep-secs", help="Path to the output migration commands file.", default=10)
    
    args = parser.parse_args()
    clients = read_csv_lines(args.inputfilename)

    script_filepath = "/home/smartechro/producer_consumer_Selection2.sh"
    producer_methods = ["readEngagementEventsWithMetaKey","readUserDetailsWithMetaKey","readUserAttributes","readAnonUserAttributes","readAnonUserDetailsWithMetaKeyKafka","readAnonEngagementEvents","readDisableUserAttributes","readDisableUserDetailsWithMetaKeyKafka","readDisableEngagementEvents"]
    consumer_methods = ["writeEngagementEventsToUserEvents","writeUserDetailsToUserEvents","writeUserAttributes","writeAnonUserAttributes","writeAnonUserDetailsToAnonUserEvents","writeAnonEngagementEventsToAnonUserEvents","writeDisableUserAttributes","writeDisableUserDetailsToDisableUserEvents","writeDisableEngagementEventsToDisabledUserEvents"]
    sleep_secs = args.sleep_secs

    create_migration_commands(script_filepath, args.outputdirpath, producer_methods, consumer_methods, clients, sleep_secs)
    