import os
import sys
import json
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

import gspread
from google.oauth2.service_account import Credentials


ET = ZoneInfo("America/New_York")
BASE = "https://stats.nba.com/stats"

HEADERS = {
    "User-Agent": os.environ.get(
        "NBA_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Connection": "keep-alive",
}


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


def nba_get(endpoint, params, timeout=120, retries=6, backoff=2.0):
    url = f"{BASE}/{endpoint}"
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            # NBA sometimes returns 429/403 intermittently
            if r.status_code in (429, 403, 502, 503):
                raise requests.HTTPError(f"HTTP {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = backoff * (i + 1)
            time.sleep(sleep_s)
    raise RuntimeError(f"NBA request failed after {retries} retries: {last_err}")


def resultset_to_df(payload, idx=0):
    rs = payload["resultSets"][idx]
    headers = rs["headers"]
    rows = rs["rowSet"]
    return pd.DataFrame(rows, columns=headers)


def parse_opponent_from_matchup(matchup):
    # "LAL vs BOS" or "LAL @ BOS" -> opponent is last token
    return matchup.replace("vs.", "vs").split()[-1]


def top_bottom_10(df, col):
    df = df.sort_values(col, ascending=False).reset_index(drop=True)
    top = df.head(10).copy()
    top.insert(0, "RANK", range(1, len(top) + 1))
    top.insert(1, "GROUP", "MOST ALLOWED")

    bottom = df.tail(10).sort_values(col, ascending=True).copy()
    bottom.insert(0, "RANK", range(1, len(bottom) + 1))
    bottom.insert(1, "GROUP", "LEAST ALLOWED")

    return pd.concat([top, bottom], ignore_index=True)


def write_sheet(ws, df):
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())


def main():
    strict_mwf_10am_et_guard()

    t = now_et()
    season = os.environ.get("SEASON") or current_season_str(t)
    season_type = os.environ.get("SEASON_TYPE", "Regular Season")
    last_n = int(os.environ.get("LAST_N_GAMES_PER_TEAM", "10"))

    # --- TEAM GAME LOGS (to get each team’s last N games) ---
    team_payload = nba_get(
        "leaguegamelog",
        params={
            "Counter": "0",
            "Direction": "DESC",
            "LeagueID": "00",
            "PlayerOrTeam": "T",
            "Season": season,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
    )
    teams = resultset_to_df(team_payload)

    # TEAM_ABBREVIATION, GAME_ID, GAME_DATE
    teams["GAME_DATE"] = pd.to_datetime(teams["GAME_DATE"])
    teams = teams.sort_values(["TEAM_ABBREVIATION", "GAME_DATE"])
    last_games = teams.groupby("TEAM_ABBREVIATION").tail(last_n)
    team_games = last_games.groupby("TEAM_ABBREVIATION")["GAME_ID"].apply(lambda s: set(s.astype(str))).to_dict()

    # --- PLAYER GAME LOGS (all players, then filter to each DEF team's last N games) ---
    player_payload = nba_get(
        "leaguegamelog",
        params={
            "Counter": "0",
            "Direction": "DESC",
            "LeagueID": "00",
            "PlayerOrTeam": "P",
            "Season": season,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
    )
    players = resultset_to_df(player_payload)

    # Required columns
    for c in ["GAME_ID", "MATCHUP", "PLAYER_ID", "PTS", "AST", "REB"]:
        if c not in players.columns:
            raise RuntimeError(f"Missing column {c} in player logs. Columns: {list(players.columns)}")

    players["GAME_ID"] = players["GAME_ID"].astype(str)
    players["DEF_TEAM"] = players["MATCHUP"].astype(str).apply(parse_opponent_from_matchup)

    # Try to find a position-like column in player logs
    pos_col = None
    for c in ["PLAYER_POSITION", "POSITION", "POS"]:
        if c in players.columns:
            pos_col = c
            break

    if pos_col is None:
        # If position isn't present, we cannot do position splits without another endpoint.
        # Fail with a clear message.
        raise RuntimeError(
            "Player game logs did not include a POSITION column. "
            "We need a position source. Reply and I'll switch to a lightweight roster endpoint "
            "or a manual mapping file."
        )

    # Normalize to G/F/C buckets (most consistent)
    def norm_pos(p):
        p = str(p).upper()
        if "G" in p:
            return "G"
        if "F" in p:
            return "F"
        if "C" in p:
            return "C"
        return "UNK"

    players["POS"] = players[pos_col].apply(norm_pos)

    # Filter to each defensive team's last N games
    players = players[players.apply(lambda r: r["GAME_ID"] in team_games.get(r["DEF_TEAM"], set()), axis=1)]
    players = players[players["POS"].isin(["G", "F", "C"])].copy()

    # Sum per (DEF_TEAM, POS, GAME_ID) then average across games
    per_game = players.groupby(["DEF_TEAM", "POS", "GAME_ID"], as_index=False)[["PTS", "AST", "REB"]].sum()
    allowed = per_game.groupby(["DEF_TEAM", "POS"], as_index=False)[["PTS", "AST", "REB"]].mean()

    # Build top/bottom sheets
    sheets = {}
    def rank(pos, stat):
        d = allowed[allowed["POS"] == pos][["DEF_TEAM", stat]].copy()
        d = d.rename(columns={"DEF_TEAM": "TEAM", stat: f"{stat}_ALLOWED"})
        return top_bottom_10(d, f"{stat}_ALLOWED")

    sheets["G_PTS"] = rank("G", "PTS")
    sheets["G_AST"] = rank("G", "AST")
    sheets["G_REB"] = rank("G", "REB")
    sheets["F_PTS"] = rank("F", "PTS")
    sheets["F_REB"] = rank("F", "REB")
    sheets["C_PTS"] = rank("C", "PTS")
    sheets["C_REB"] = rank("C", "REB")

    # Write Excel artifact
    os.makedirs("output", exist_ok=True)
    out_path = f"output/nba_allowed_by_position_last{last_n}_{t.strftime('%Y%m%d_%H%M')}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta = pd.DataFrame([{
            "generated_at_et": t.isoformat(),
            "season": season,
            "season_type": season_type,
            "last_n_games_per_team": last_n
        }])
        meta.to_excel(writer, sheet_name="README", index=False)
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

    # Update Google Sheet (optional)
    if os.environ.get("GSERVICE_JSON") and os.environ.get("GOOGLE_SHEET_ID"):
        creds = Credentials.from_service_account_info(
            json.loads(os.environ["GSERVICE_JSON"]),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

        for name, df in sheets.items():
            try:
                ws = sh.worksheet(name)
            except gspread.exceptions.WorksheetNotFound:
                ws = sh.add_worksheet(title=name, rows=200, cols=20)
            write_sheet(ws, df)

    print("SUCCESS")


if __name__ == "__main__":
    main()
