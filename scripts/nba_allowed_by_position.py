import os
import sys
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from nba_api.stats.endpoints import leaguegamelog, leaguedashplayerbiostats

import gspread
from google.oauth2.service_account import Credentials
import inspect

def safe_endpoint(endpoint_cls, **kwargs):
    """
    Create an nba_api endpoint instance, but only pass kwargs that the installed
    nba_api version actually supports.
    """
    sig = inspect.signature(endpoint_cls.__init__)
    allowed = set(sig.parameters.keys())
    filtered = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    return endpoint_cls(**filtered)
    
ET = ZoneInfo("America/New_York")


def now_et():
    return datetime.now(timezone.utc).astimezone(ET)


def current_season_str(today_et: datetime) -> str:
    year = today_et.year
    start = year if today_et.month >= 10 else year - 1
    return f"{start}-{str(start + 1)[-2:]}"


def strict_mwf_10am_et_guard():
    """
    Normal scheduled runs should only output at exactly 10:00 AM ET on Mon/Wed/Fri.
    Manual runs can set FORCE_RUN=1 to bypass this so you can "run today".
    """
    if os.environ.get("FORCE_RUN", "").strip() == "1":
        return

    t = now_et()
    if t.weekday() not in (0, 2, 4):  # Mon/Wed/Fri
        sys.exit(0)
    if t.hour != 10:
        sys.exit(0)


def parse_opponent_from_matchup(matchup: str) -> str:
    parts = matchup.replace("vs.", "vs").split()
    return parts[-1]


def normalize_position(pos: str) -> str:
    p = (pos or "").upper().strip()
    if "G" in p:
        return "G"
    if "F" in p:
        return "F"
    if "C" in p:
        return "C"
    return "UNK"


def top_bottom_10(df: pd.DataFrame, stat_col: str) -> pd.DataFrame:
    df = df.sort_values(stat_col, ascending=False).reset_index(drop=True)
    top = df.head(10).copy()
    top.insert(0, "RANK", range(1, len(top) + 1))
    top.insert(1, "GROUP", "MOST ALLOWED")

    bottom = df.tail(10).sort_values(stat_col, ascending=True).copy()
    bottom.insert(0, "RANK", range(1, len(bottom) + 1))
    bottom.insert(1, "GROUP", "LEAST ALLOWED")

    return pd.concat([top, bottom], ignore_index=True)


def write_sheet_tab(ws, df: pd.DataFrame):
    ws.clear()
    values = [df.columns.tolist()] + df.fillna("").values.tolist()
    ws.update(values)


def get_gsheet_client():
    sa_json = os.environ.get("GSERVICE_JSON", "").strip()
    if not sa_json:
        return None

    creds_info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


def main():
    strict_mwf_10am_et_guard()

    t = now_et()
    season = os.environ.get("SEASON", "").strip() or current_season_str(t)
    season_type = os.environ.get("SEASON_TYPE", "Regular Season")
    last_n_games_per_team = int(os.environ.get("LAST_N_GAMES_PER_TEAM", "10"))

    print(f"Season={season} | SeasonType={season_type} | LastNGamesPerTeam={last_n_games_per_team}")

    # 1) Player positions
    bio = safe_endpoint(
    leaguedashplayerbiostats.LeagueDashPlayerBioStats,
    season=season,
    season_type_all_star=season_type,
).get_data_frames()[0]
).get_data_frames()[0]

    pos_col = None
    for c in ["PLAYER_POSITION", "POSITION", "POS"]:
        if c in bio.columns:
            pos_col = c
            break
    if pos_col is None:
        raise RuntimeError(f"Could not find a position column. Columns: {list(bio.columns)}")

    bio = bio[["PLAYER_ID", "PLAYER_NAME", pos_col]].copy()
    bio["PLAYER_ID"] = bio["PLAYER_ID"].astype(int)
    bio["POS_GROUP"] = bio[pos_col].astype(str).map(normalize_position)

    # 2) Team game logs -> last N games per team
   team_logs = safe_endpoint(
    leaguegamelog.LeagueGameLog,
    season=season,
    season_type_all_star=season_type,
    player_or_team_abbreviation="T",
).get_data_frames()[0]

    team_logs["GAME_DATE"] = pd.to_datetime(team_logs["GAME_DATE"])
    team_logs = team_logs.sort_values(["TEAM_ID", "GAME_DATE"])
    team_last = team_logs.groupby("TEAM_ID").tail(last_n_games_per_team)

    team_games_map = (
        team_last.groupby("TEAM_ABBREVIATION")["GAME_ID"]
        .apply(lambda s: set(s.astype(str)))
        .to_dict()
    )

    # 3) Player game logs -> filter to each defensive team’s last N games
    player_logs = safe_endpoint(
    leaguegamelog.LeagueGameLog,
    season=season,
    season_type_all_star=season_type,
    player_or_team_abbreviation="P",
).get_data_frames()[0]

    needed = ["GAME_ID", "MATCHUP", "PLAYER_ID", "PTS", "AST", "REB"]
    player_logs = player_logs[needed].copy()
    player_logs["GAME_ID"] = player_logs["GAME_ID"].astype(str)
    player_logs["PLAYER_ID"] = player_logs["PLAYER_ID"].astype(int)
    player_logs["DEF_TEAM_ABBR"] = player_logs["MATCHUP"].astype(str).map(parse_opponent_from_matchup)

    def in_last_n(row):
        games = team_games_map.get(row["DEF_TEAM_ABBR"])
        return (games is not None) and (row["GAME_ID"] in games)

    player_logs = player_logs[player_logs.apply(in_last_n, axis=1)].copy()

    # 4) Join positions
    merged = player_logs.merge(bio[["PLAYER_ID", "POS_GROUP"]], on="PLAYER_ID", how="left")
    merged = merged.dropna(subset=["POS_GROUP"])
    merged = merged[merged["POS_GROUP"].isin(["G", "F", "C"])]

    # 5) Sum per (DEF_TEAM, POS_GROUP, GAME_ID) -> average across games
    per_game = (
        merged.groupby(["DEF_TEAM_ABBR", "POS_GROUP", "GAME_ID"], as_index=False)[["PTS", "AST", "REB"]]
        .sum()
    )

    allowed = (
        per_game.groupby(["DEF_TEAM_ABBR", "POS_GROUP"], as_index=False)[["PTS", "AST", "REB"]]
        .mean()
        .rename(columns={"PTS": "PTS_ALLOWED", "AST": "AST_ALLOWED", "REB": "REB_ALLOWED"})
    )

    # Build sheets
    sheets = {}

    def build_rank(pos, stat):
        df = allowed[allowed["POS_GROUP"] == pos][["DEF_TEAM_ABBR", f"{stat}_ALLOWED"]].copy()
        df = df.rename(columns={"DEF_TEAM_ABBR": "TEAM"})
        return top_bottom_10(df, f"{stat}_ALLOWED")

    sheets["G_PTS"] = build_rank("G", "PTS")
    sheets["G_AST"] = build_rank("G", "AST")
    sheets["G_REB"] = build_rank("G", "REB")
    sheets["F_PTS"] = build_rank("F", "PTS")
    sheets["F_REB"] = build_rank("F", "REB")
    sheets["C_PTS"] = build_rank("C", "PTS")
    sheets["C_REB"] = build_rank("C", "REB")

    # Write Excel
    out_dir = os.environ.get("OUTPUT_DIR", "output")
    os.makedirs(out_dir, exist_ok=True)
    stamp = t.strftime("%Y-%m-%d_%H%M_ET")
    out_path = os.path.join(out_dir, f"nba_allowed_by_position_last{last_n_games_per_team}_{season}_{stamp}.xlsx")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        meta = pd.DataFrame([{
            "generated_at_et": t.isoformat(),
            "season": season,
            "season_type": season_type,
            "last_n_games_per_team": last_n_games_per_team,
            "note": "Positions are NBA.com Stats API groupings (G/F/C). Allowed stats computed from opponent player game logs."
        }])
        meta.to_excel(writer, sheet_name="README", index=False)
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)

    print(f"Wrote Excel: {out_path}")

    # Update Google Sheet (if secrets set)
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "").strip()
    gc = get_gsheet_client()
    if gc and sheet_id:
        sh = gc.open_by_key(sheet_id)

        desired_tabs = ["README"] + list(sheets.keys())
        existing_titles = {ws.title for ws in sh.worksheets()}

        for tab in desired_tabs:
            if tab not in existing_titles:
                sh.add_worksheet(title=tab, rows=200, cols=20)

        readme_ws = sh.worksheet("README")
        readme_df = pd.DataFrame([{
            "generated_at_et": t.isoformat(),
            "season": season,
            "season_type": season_type,
            "last_n_games_per_team": last_n_games_per_team
        }])
        write_sheet_tab(readme_ws, readme_df)

        for tab, df in sheets.items():
            ws = sh.worksheet(tab)
            write_sheet_tab(ws, df)

        print("Updated Google Sheet successfully.")
    else:
        print("Skipped Google Sheets update (missing GSERVICE_JSON and/or GOOGLE_SHEET_ID).")


if __name__ == "__main__":
    main()
