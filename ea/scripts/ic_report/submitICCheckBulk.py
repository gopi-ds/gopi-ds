import requests
import json
import os
import shutil
import time
from jsmin import jsmin
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file_path):
    """
    Load and parse the JSON configuration file.
    :param config_file_path: The path to the JSON configuration file.
    :return: The configuration dictionary.
    """
    try:
        with open(config_file_path, 'r') as config_file:
            minified_json = jsmin(config_file.read())
            return json.loads(minified_json)
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return None

def get_oauth_token(config):
    """
    Get an OAuth token from the specified endpoint.
    :param config: Configuration dictionary with token_url, client_id, client_secret, and optional scope.
    :return: The access token.
    """
    required_fields = ['token_url', 'client_id', 'client_secret']
    for field in required_fields:
        if field not in config:
            logging.error(f"Missing required configuration field: {field}")
            return None

    payload = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
    }
    if 'scope' in config:
        payload['scope'] = config['scope']

    try:
        response = requests.post(config['token_url'], data=payload)
        response.raise_for_status()
        token_response = response.json()
        access_token = token_response.get('access_token')

        if not access_token:
            logging.error(f"Failed to retrieve access token: {token_response}")
            return None

        return access_token
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")
        return None

def make_post_request(api_url, access_token, file_path, retries=3, wait_time=30):
    """
    Make a POST API request using the Bearer token for authorization with retries.
    If the token expires, get a new one and retry the request.
    :param api_url: The URL of the API endpoint.
    :param access_token: The access token for authorization.
    :param file_path: Path to a file containing the JSON data to be sent in the request body.
    :param retries: Number of retries before giving up.
    :param wait_time: Time to wait (in seconds) between retries.
    :return: The API response or None if all retries failed.
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',  # Specify that we are sending JSON data
        'Accept': 'application/json'  # Expect JSON response
    }

    for attempt in range(retries):
        try:
            # Read the JSON content from the file and send it as the POST body
            with open(file_path, 'r') as file:
                json_data = json.load(file)  # Assuming the file contains a JSON object

            response = requests.post(api_url, headers=headers, json=json_data)
            if response.status_code == 401:  # Token expired or unauthorized
                logging.warning(f"Token expired for {file_path}")
                return "expired"
            response.raise_for_status()  # Raise exception if the request fails for other reasons
            return response.json()  # Return the response as JSON

        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)  # Wait before retrying
            else:
                logging.error(f"Max retries reached for {file_path}.")
                return None

def process_files_in_folder(api_url, config, folder_path, success_folder):
    """
    Process all files in the specified folder by making API POST requests for each file.
    Move the file to a success folder if the API request is successful.

    :param api_url: The URL of the API endpoint.
    :param config: Configuration dictionary to request new token if needed.
    :param folder_path: The path to the folder containing JSON files.
    :param success_folder: The path to the folder where successful files are moved.
    """
    try:
        # List all JSON files in the folder
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        if not json_files:
            logging.warning("No JSON files found in the folder.")
            return

        # Get initial token once
        access_token = get_oauth_token(config)
        if not access_token:
            logging.error("Failed to retrieve initial access token.")
            return

        for file_name in json_files:
            file_path = os.path.join(folder_path, file_name)
            logging.info(f"Processing file: {file_path}")
            api_response = make_post_request(api_url, access_token, file_path)

            # If token expired, get a new token and retry once
            if api_response == "expired":
                logging.info(f"Refreshing token for {file_name}")
                access_token = get_oauth_token(config)
                if access_token:
                    api_response = make_post_request(api_url, access_token, file_path)
                else:
                    logging.error("Failed to refresh access token.")
                    continue  # Skip this file if token refresh fails

            if api_response:
                logging.info(f"API response for {file_name}: {json.dumps(api_response, indent=2)}")
                # Optionally write the response to a file
                with open(f"output_{file_name}.json", 'w') as output_file:
                    json.dump(api_response, output_file, indent=4)

                # Move the file to the success folder after processing
                destination_path = os.path.join(success_folder, file_name)
                shutil.move(file_path, destination_path)
                logging.info(f"Moved {file_name} to {success_folder}")
            else:
                logging.error(f"Failed to obtain API response for {file_name}.")
    except Exception as e:
        logging.error(f"Error processing files in folder: {e}")

if __name__ == "__main__":

    # Load configuration from config.json
    config = load_config('./config/icCheckBulkConfig.json')

    if config:
        # Create the success folder if it doesn't exist
        success_folder = config['success_folder']
        if not os.path.exists(success_folder):
            os.makedirs(success_folder)

        # Process all JSON files in the specified folder and move successful ones to success folder
        process_files_in_folder(config['api_url'], config, config['folder_path'], success_folder)
    else:
        logging.error("Failed to load configuration.")
