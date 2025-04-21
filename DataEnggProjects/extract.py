import requests

def download_csv_from_url(url, destination_file_name):
    """
    Downloads a CSV file from a public URL.

    Args:
        url (str): The public URL of the CSV file.
        destination_file_name (str): Local path to save the downloaded file.
    """
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

        # Write the content to a local file
        with open(destination_file_name, 'wb') as file:
            file.write(response.content)

        print(f"File downloaded to {destination_file_name}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Replace this with your public URL and destination path
    public_url = "https://storage.googleapis.com/uber-data-engineering-projects-arv/uber_data.csv"
    destination_file_name = "uber_data_extract.csv"

    download_csv_from_url(public_url, destination_file_name)
