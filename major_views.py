"""_summary_

    Returns:
        _type_: _description_
    """

from re import sub

from dotenv import dotenv_values, load_dotenv, set_key
from duckdb import connect
from requests import get, post

con = connect(database="major_copenhagen_2024.db")


load_dotenv()


def get_head():
    """_summary_

    Returns:
        _type_: _description_
    """
    head_auth = {
        "Authorization": "Bearer " + dotenv_values()["ACESS_TOKEN"],
        "Client-Id": dotenv_values()["CLIENT_ID"],
    }
    return head_auth


head = get_head()
request = get(
    url="https://api.twitch.tv/helix/streams",
    params={"game_id": "32399"},
    headers=head,
    timeout=5,
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


def analyser_stream(id_stream):
    """_summary_

    Returns:
        _type_: _description_
    """
    return con.execute(f"SELECT id from stream where id = {id_stream}").fetchone()


def insert_stream(id_streamer, title, started_at, id_stream):
    """_summary_

    Returns:
        _type_: _description_
    """
    query_insert = f""" INSERT INTO major_copenhagen_2024.main.stream
    (id_streamer, title, started_at, id)
    VALUES({id_streamer}, '{title}', '{started_at}', {id_stream})"""
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
    query_insert = f""" INSERT INTO major_copenhagen_2024.main.views_stream
            (id_stream, viewer_count)
            VALUES({id_stream}, '{viewer_count}')"""
    con.execute(query_insert)


def validate_token():
    """_summary_

    Returns:
        _type_: _description_
    """
    acess_token = dotenv_values()["ACESS_TOKEN"]
    head_validation = {"Authorization": "OAuth " + acess_token}
    response = get(
        url="https://id.twitch.tv/oauth2/token", headers=head_validation, timeout=5
    )
    return response


def get_new_token():
    """_summary_

    Returns:
        _type_: _description_
    """
    client_id = dotenv_values()["CLIENT_ID"]
    client_secret = dotenv_values()["CLIENT_SECRET"]

    params = f"client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"
    token = post(url="https://id.twitch.tv/oauth2/token", params=params, timeout=5)
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

        title_stream = sub(r'[.,"\'-?:!;]', "", streamer["title"])
        started_at_stream = streamer["started_at"]
        id_stream_request = streamer["id"]

        viewer_count_stream = streamer["viewer_count"]

        if not analyser_streamer(user_id=user_id_streamer):
            insert_streamer(
                language=language_streamer,
                user_id=user_id_streamer,
                user_name=user_name_streamer,
            )

        if not analyser_stream(id_stream=id_stream_request):
            insert_stream(
                id_stream=id_stream_request,
                id_streamer=user_id_streamer,
                started_at=started_at_stream,
                title=title_stream,
            )

        if not analyser_stream_view(
            id_stream=id_stream_request, viewer_count=viewer_count_stream
        ):
            insert_stream_view(
                id_stream=id_stream_request, viewer_count=viewer_count_stream
            )
