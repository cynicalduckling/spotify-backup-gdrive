from dotenv import load_dotenv
import spotipy as spotify
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import numpy

load_dotenv()


def clean_user_playlist_info(playlists):
    return {
        "name": playlists["name"],
        "id": playlists["id"],
        "visibility": "Public" if playlists["public"] else "Private",
        "spotify_url": playlists["external_urls"]["spotify"],
        "id": playlists["id"],
    }


def get_all_user_playlists(spotify_client_object):
    final = []
    for offset in range(0, 10000, 50):
        result = list(
            map(
                clean_user_playlist_info,
                spotify_client_object.current_user_playlists(offset=offset)["items"],
            )
        )
        if len(result) == 0:
            break
        final = final + result

    return final


def get_playlist_tracks(spotify_client_object, playlist_id):
    final = []
    for offset in range(0, 10000, 100):
        result = spotify_client_object.playlist_items(
            playlist_id=playlist_id,
            fields="items(track(album(artists.name, name, id, release_date, external_urls.spotify), artists(name, id, external_urls.spotify), duration_ms, id, external_urls.spotify, name, popularity))",
            offset=offset,
        )
        if len(result["items"]) == 0:
            break
        final = final + result["items"]

    return final


def create_playlist_dataframe(track_list, playlist_info):

    count = 0
    df = pd.DataFrame()
    try:
        for entry in track_list:
            to_append = {
                "playlist_name": playlist_info["name"],
                "playlist_url": playlist_info["spotify_url"],
                "playlist_id": playlist_info["id"],
                "track_name": entry["track"]["name"],
                "track_artists_all": ", ".join(
                    [x["name"] for x in entry["track"]["artists"]]
                ),
                "track_main_artist": entry["track"]["artists"][0]["name"],
                "track_main_artist_id": entry["track"]["artists"][0]["id"],
                "track_main_artist_url": entry["track"]["artists"][0]["external_urls"][
                    "spotify"
                ],
                "track_url": entry["track"]["external_urls"]["spotify"],
                "track_id": entry["track"]["id"],
                "track_popularity": entry["track"]["popularity"],
                "track_length_ms": entry["track"]["duration_ms"],
                "album_name": entry["track"]["album"]["name"],
                "album_release_date": entry["track"]["album"]["release_date"],
                "album_artist": entry["track"]["album"]["artists"][0]["name"],
                "album_url": entry["track"]["album"]["external_urls"]["spotify"],
                "album_id": entry["track"]["album"]["id"],
            }

            df_append = pd.DataFrame(data=to_append, index=[0])
            df = pd.concat([df, df_append])
    except Exception as e:
        count = count + 1

    if count > 0:
        print("exception at creating dataframe")
        print(f"{count} tracks raised errors")

    return df


def get_genre_list(spotify_client_object, artist_id_list):
    artists = []
    try:
        for offset in range(0, 1000, 25):
            temp_artists = spotify_client_object.artists(
                artist_id_list[offset : offset + 25]
            )
            artists = artists + temp_artists["artists"]
            if len(artist_id_list) <= offset + 25:
                break
    except Exception as e:
        print("exception at getting genres")
        print(offset, len(artist_id_list))

    final = [", ".join(x) for x in list(map(lambda x: x["genres"], artists))]
    return final
