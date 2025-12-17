import os
import sys
import json
import inspect
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from nba_api.stats.endpoints import leaguegamelog, leaguedashplayerbiostats

import gspread
from google.oauth2.service_account import Credentials


ET = ZoneInfo("America/New_York")


def now_et():
    return datetime.now(timezone.utc).astimezone(ET)


def current_season_str(today_et):
    year = today_et.year
    start = year if today_et.month >= 10 else year - 1
    return f"{start}-{str(start + 1)[-2:]}"


def strict_mwf_10am_et_guard():
    if os.environ.get("FORCE_RUN") == "1":
        return
    t = now_et()
    if t.weekday() not in (0, 2, 4):
        sys.exit(0)
    if t.hour != 10:
        sys.exit(0)


def parse_opponent_from_matchup(matchup):
    return matchup.replace("vs.", "vs").split()[-1]


def normalize_position(pos):
    p = (pos or "").upper()
    if "G" in p:
        return "G"
    if "F" in p:
        return "F"
    if "C" in p:
        return "C"
    return "UNK"


def safe_endpoint(endpoint_cls, **kwargs):
    sig = inspect.signature(endpoint_cls.__init__)
    allowed = set(sig.parameters.keys())
    filtered = {k: v for k, v in kwargs.items() if k in allowed}
    return endpoint_cls(**filtered)


def top_bottom_10(df, col):
    df = df.sort_values(col, ascending=False).reset_index(drop=True)
    top = df.head(10).copy()
    top.insert(0, "RANK", range(1, 11))
    top.insert(1, "GROUP", "MOST ALLOWED")

    bottom = df.tail(10).sort_values(col).copy()
    bottom.insert(0, "RANK", range(1, 11))
    bottom.insert(1, "GROUP", "LEAST ALLOWED")

    return pd.concat([top, bottom], ignore_index=True)


def write_sheet(ws, df):
    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())


def main():
    strict_mwf_10am_et_guard()

    t = now_et()
    season = os.environ.get("SEASON") or current_season_str(t)
    season_type = os.environ.get("SEASON_TYPE", "Regular Season")
    last_n = int(os.environ.get("LAST_N_GAMES_PER_TEAM", "10"))

    # PLAYER POSITIONS
    bio = safe_endpoint(
        leaguedashplayerbiostats.LeagueDashPlayerBioStats,
        season=season,
        season_type_all_star=season_type,
    ).get_data_frames()[0]

    pos_col = next(c for c in ["PLAYER_POSITION", "POSITION", "POS"] if c in bio.columns)
    bio = bio[["PLAYER_ID", pos_col]].copy()
    bio["POS"] = bio[pos_col].apply(normalize_position)

    # TEAM LAST N GAMES
    teams = safe_endpoint(
        leaguegamelog.LeagueGameLog,
        season=season,
        season_type_all_star=season_type,
        player_or_team_abbreviation="T",
    ).get_data_frames()[0]

    teams["GAME_DATE"] = pd.to_datetime(teams["GAME_DATE"])
    last_games = teams.sort_values("GAME_DATE").groupby("TEAM_ABBREVIATION").tail(last_n)
    team_games = last_games.groupby("TEAM_ABBREVIATION")["GAME_ID"].apply(set).to_dict()

    # PLAYER GAME LOGS
    players = safe_endpoint(
        leaguegamelog.LeagueGameLog,
        season=season,
        season_type_all_star=season_type,
        player_or_team_abbreviation="P",
    ).get_data_frames()[0]

    players["DEF_TEAM"] = players["MATCHUP"].apply(parse_opponent_from_matchup)
    players = players[players.apply(lambda r: r["GAME_ID"] in team_games.get(r["DEF_TEAM"], set()), axis=1)]

    merged = players.merge(bio, on="PLAYER_ID")
    merged = merged[merged["POS"].isin(["G", "F", "C"])]

    per_game = merged.groupby(["DEF_TEAM", "POS", "GAME_ID"])[["PTS", "AST", "REB"]].sum().reset_index()
    allowed = per_game.groupby(["DEF_TEAM", "POS"])[["PTS", "AST", "REB"]].mean().reset_index()

    sheets = {
        "G_PTS": top_bottom_10(allowed[allowed["POS"] == "G"][["DEF_TEAM", "PTS"]], "PTS"),
        "G_AST": top_bottom_10(allowed[allowed["POS"] == "G"][["DEF_TEAM", "AST"]], "AST"),
        "G_REB": top_bottom_10(allowed[allowed["POS"] == "G"][["DEF_TEAM", "REB"]], "REB"),
        "F_PTS": top_bottom_10(allowed[allowed["POS"] == "F"][["DEF_TEAM", "PTS"]], "PTS"),
        "F_REB": top_bottom_10(allowed[allowed["POS"] == "F"][["DEF_TEAM", "REB"]], "REB"),
        "C_PTS": top_bottom_10(allowed[allowed["POS"] == "C"][["DEF_TEAM", "PTS"]], "PTS"),
        "C_REB": top_bottom_10(allowed[allowed["POS"] == "C"][["DEF_TEAM", "REB"]], "REB"),
    }

    os.makedirs("output", exist_ok=True)
    path = f"output/nba_allowed_by_position_{t.strftime('%Y%m%d_%H%M')}.xlsx"

    with pd.ExcelWriter(path) as writer:
        for name, df in sheets.items():
            df.rename(columns={"DEF_TEAM": "TEAM"}).to_excel(writer, sheet_name=name, index=False)

    # GOOGLE SHEETS
    if os.environ.get("GSERVICE_JSON") and os.environ.get("GOOGLE_SHEET_ID"):
        creds = Credentials.from_service_account_info(
            json.loads(os.environ["GSERVICE_JSON"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

        for name, df in sheets.items():
            try:
                ws = sh.worksheet(name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=name, rows=200, cols=20)
            write_sheet(ws, df.rename(columns={"DEF_TEAM": "TEAM"}))

    print("SUCCESS")


if __name__ == "__main__":
    main()
