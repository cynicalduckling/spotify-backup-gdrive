import pandas as pd
import re, pytz, io, warnings, time, os, json
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from datetime import datetime
import spotify_functions as spotify
import spotipy as spotipy
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
IST = pytz.timezone("Asia/Kolkata")
warnings.filterwarnings("ignore")
start = time.time()


def now():
    return datetime.now(IST).strftime("%b %e, %Y %H:%M")


print(f"\nStarted executing at {now()}")


def drive_upload(df, drive, name):
    file = drive.CreateFile(
        {
            "parents": [{"id": "1_urOyu2ViM4CRx8mHDpR1B0_qEgRKsGf"}],
            "title": name,
        }
    )

    df_byte = io.BytesIO()
    df.to_excel(df_byte, index=False, sheet_name="Playlist")

    file.content = df_byte

    file.Upload()


def delete_old_files_from_drive(file_list):
    [x.Delete() for x in tqdm(file_list)]


def main():
    SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
    SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

    print("\n---> Authenticating with Spotify")

    scope = [
        "user-library-read",
        "user-read-playback-state",
        "app-remote-control",
        "user-modify-playback-state",
        "playlist-read-private",
        "playlist-read-collaborative",
        "playlist-modify-private",
        "playlist-modify-public",
        "user-top-read",
        "user-read-recently-played",
    ]

    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            scope=scope,
            redirect_uri="http://127.0.0.1:9090",
        )
    )

    time.sleep(1)

    print("\n---> Authenticating with Google Drive")

    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    query = {"q": f"title contains '_spotifyid'"}
    files = drive.ListFile(query).GetList()

    print("\n---> Getting user's playlists' info")

    my_playlists = spotify.get_all_user_playlists(sp)
    my_playlists = sorted(my_playlists, key=lambda x: x["name"])

    all_df = []

    etl_datetime = now()

    time.sleep(1)

    f = open("popularity.json", "w", encoding="utf-8")

    pop = {}

    print("\n---> Preparing, procesing and uploading dataframes")

    for playlist in tqdm(my_playlists):
        df = spotify.create_playlist_dataframe(
            spotify.get_playlist_tracks(sp, playlist["id"]), playlist
        )
        name = re.sub(r"[^a-zA-Z0-9 ]", "", playlist["name"])[0:30]
        artist_id_list = df["track_main_artist_id"].tolist()
        genre_list = (
            spotify.get_genre_list(sp, artist_id_list=artist_id_list)
            if len(artist_id_list) > 0
            else None
        )
        df["genre_list"] = list(genre_list)

        df["etl_datetime"] = etl_datetime

        pop[playlist["name"]] = round(df["track_popularity"].mean(), 1)
        
        drive_upload(df, drive, f"{name}_spotifyid.xlsx")
        all_df.append(df)
        df = None

    total = (
        pd.concat(all_df).drop_duplicates(subset=["track_id"]).reset_index(drop=True)
    )

    pop = dict(sorted(pop.items(), key=lambda x: x[1]))

    json.dump(pop, f, indent=4)

    f.close()

    drive_upload(total, drive, f"total_unique_tracks_spotifyid.xlsx")

    print("\n---> Deleting previous backup files")

    delete_old_files_from_drive(files) if list(total.shape)[0] > 3000 else None 

    end = time.time()

    print(
        f"\n---> Finished execution -- Execution time: {round((end-start)/60, 1)} minutes\n"
    )

    time.sleep(5)


def lambda_handler(event, context):
    main()


lambda_handler(None, None)
