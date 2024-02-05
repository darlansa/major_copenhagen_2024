import requests
from dotenv import load_dotenv, dotenv_values



load_dotenv()

client_id = dotenv_values()['CLIENT_ID']
client_secret = dotenv_values()['CLIENT_SECRET']

params = f'client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials'


token = requests.post(url='https://id.twitch.tv/oauth2/token',params=params)

acess_token = token.json()['access_token']

head = {'Authorization':'Bearer ' + acess_token,'Client-Id':client_id}

request = requests.get(url='https://api.twitch.tv/helix/streams',params={'game_id':'32399'},headers=head)


