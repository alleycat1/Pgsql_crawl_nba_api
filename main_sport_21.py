import psycopg2
from nba_api.stats.static import players
import requests

def download_photo(url, save_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print("Photo downloaded and saved successfully.")
    else:
        print("Failed to download the photo.")

try:
    all_players = players.get_players()

    for player in all_players:
        player_id = player.get('id')
        photo_url = "http://cdn.nba.com/headshots/nba/latest/1040x760/" + str(player_id) + ".png"
        save_location = "/root/scraper/" + str(player_id) + ".png"
        download_photo(photo_url, save_location)

except Exception as e:
    # Code to handle any other exceptions
    print("An error occurred:", str(e))