"""

    Submit IC Check

    Sample config Json
    {
      "token_url": "https://uaa.smarsh.cloud/oauth/token", // OAuth token endpoint
      "client_id": "app-client", // Client ID
      "client_secret": "tecfAgLk2zadPI78923SoIZfdLXdHv", // Client Secret
      "api_url": "https://archive-recon/archive/ic/prod/verify", // API endpoint URL
      "file_path": "./config/icCheckConfigQuery.json" // Path to the file to be sent
    }

    Sample icCheckConfigQuery.json
    {
        "gcids": [
            "evmigration-ffaecba5-4bab-485c-8fe8-178cb1d6faf7@VerQuMessageForge1.0_1506717413000",
            "evmigration-e27a4a78-ace1-4d79-ba33-863336e58a26@VerQuMessageForge1.0_1509200494000"
        ]
    }

"""

import requests
import json
from jsmin import jsmin


def load_config(config_file_path):
    """
    Load and parse the JSON configuration file.
    :param config_file_path: The path to the JSON configuration file.
    :return: The configuration dictionary.
    """
    with open(config_file_path, 'r') as config_file:
        minified_json = jsmin(config_file.read())
        return json.loads(minified_json)


def get_oauth_token(config):
    """
    Get an OAuth token from the specified endpoint.
    :param config: Configuration dictionary with token_url, client_id, client_secret, and optional scope.
    :return: The access token.
    """
    # Prepare the payload with client credentials
    payload = {
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
    }
    if 'scope' in config:
        payload['scope'] = config['scope']
    try:
        # Make a POST request to the token endpoint
        response = requests.post(config['token_url'], data=payload)
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        # Parse the JSON response
        token_response = response.json()
        # Extract and return the access token
        access_token = token_response.get('access_token')
        return access_token
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def make_api_request(api_url, access_token, file_path):
    """
    Make an API request using the Bearer token for authorization, sending the file content as JSON.
    :param api_url: The URL of the API endpoint.
    :param access_token: The access token for authorization.
    :param file_path: The path to the file to be sent in the payload.
    :return: The API response.
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    with open(file_path, 'r') as file:
        file_content = file.read()
        try:
            response = requests.post(api_url, headers=headers, data=file_content)
            # Raise an exception if the request was unsuccessful
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None


if __name__ == "__main__":

    # Load configuration from config.json
    config = load_config('./config/icCheckConfig.json')
    # Get the OAuth token
    token = get_oauth_token(config)
    if token:
        print(f"Access token: {token}")
        # Make an API request using the Bearer token and including a file in the body
        api_response = make_api_request(config['api_url'], token, config['file_path'])
        if api_response:
            print(f"API response: {json.dumps(api_response, indent=2)}")
        else:
            print("Failed to obtain API response.")
    else:
        print("Failed to obtain access token.")
