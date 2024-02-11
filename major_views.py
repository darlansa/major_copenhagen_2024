    """_summary_

    Returns:
        _type_: _description_
    """

import re
import requests
import duckdb
from dotenv import load_dotenv, dotenv_values, set_key


con = duckdb.connect(database="major_copenhagen_2024.db")


load_dotenv()


def get_head():
    """_summary_

    Returns:
        _type_: _description_
    """
    head = {
        "Authorization": "Bearer " + dotenv_values()["ACESS_TOKEN"],
        "Client-Id": dotenv_values()["CLIENT_ID"],
    }
    return head


head = get_head()
request = requests.get(
    url="https://api.twitch.tv/helix/streams", params={"game_id": "32399"}, headers=head
)


def analyser_streamer(user_id):
    """_summary_

    Returns:
        _type_: _description_
    """
    return con.execute(f"SELECT id from streamers where id = {user_id}").fetchone()


def insert_streamer(user_id, user_name, language):
    """_summary_

    Returns:
        _type_: _description_
    """
    query_insert = f""" INSERT INTO major_copenhagen_2024.main.streamers
    (id, user_name, language) VALUES({user_id}, '{user_name}', '{language}')"""
    con.execute(query_insert)


def analyser_stream(id):
    """_summary_

    Returns:
        _type_: _description_
    """
    return con.execute(f"SELECT id from stream where id = {id}").fetchone()


def insert_stream(id_streamer, title, started_at, id):
    """_summary_

    Returns:
        _type_: _description_
    """
    query_insert = f""" INSERT INTO major_copenhagen_2024.main.stream 
    (id_streamer, title, started_at, id) VALUES({id_streamer}, '{title}', '{started_at}', {id})"""
    con.execute(query_insert)


def analyser_stream_view(id_stream, viewer_count):
    """_summary_

    Returns:
        _type_: _description_
    """
    return con.execute(
        f"""SELECT id_stream 
            from views_stream 
            where id_stream = {id_stream} 
            and viewer_count = {viewer_count}"""
    ).fetchone()


def insert_stream_view(id_stream, viewer_count):
    """_summary_

    Returns:
        _type_: _description_
    """
    query_insert = f""" INSERT INTO major_copenhagen_2024.main.views_stream (id_stream, viewer_count)
            VALUES({id_stream}, '{viewer_count}')"""
    con.execute(query_insert)


def validate_token():
    """_summary_

    Returns:
        _type_: _description_
    """
    acess_token = dotenv_values()["ACESS_TOKEN"]
    head = {"Authorization": "OAuth " + acess_token}
    response = requests.get(url="https://id.twitch.tv/oauth2/token", headers=head)
    return response


def get_new_token():
    """_summary_

    Returns:
        _type_: _description_
    """
    client_id = dotenv_values()["CLIENT_ID"]
    client_secret = dotenv_values()["CLIENT_SECRET"]

    params = f"client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"
    token = requests.post(url="https://id.twitch.tv/oauth2/token", params=params)
    acess_token = token.json()["access_token"]
    set_key("./.env", "ACESS_TOKEN", acess_token)


if validate_token().status_code != "200":
    get_new_token()

else:
    streamers = request.json()

    for streamer in streamers["data"]:
        user_id_streamer = streamer["user_id"]
        user_name_streamer = streamer["user_name"]
        language_streamer = streamer["language"]

        title_stream = re.sub(r'[.,"\'-?:!;]', "", streamer["title"])
        started_at_stream = streamer["started_at"]
        id_stream = streamer["id"]

        viewer_count_stream = streamer["viewer_count"]

        if not analyser_streamer(user_id=user_id_streamer):
            insert_streamer(
                language=language_streamer,
                user_id=user_id_streamer,
                user_name=user_name_streamer,
            )

        if not analyser_stream(id=id_stream):
            insert_stream(
                id=id_stream,
                id_streamer=user_id_streamer,
                started_at=started_at_stream,
                title=title_stream,
            )

        if not analyser_stream_view(
            id_stream=id_stream, viewer_count=viewer_count_stream
        ):
            insert_stream_view(id_stream=id_stream, viewer_count=viewer_count_stream)
