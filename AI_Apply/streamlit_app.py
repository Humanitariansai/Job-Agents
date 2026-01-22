import os
import subprocess
import pandas as pd
import streamlit as st

JOBS_CSV = "Greenhouse_Jobs.csv"
PERSONAL_CSV = "Personal_Jobs.csv"

st.set_page_config(page_title="Job Search Agent", layout="wide")
st.title("🤖 Job Search Agent (Demo)")
st.caption(f"Working directory: {os.getcwd()}")

# ------------------ State ------------------
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_top_df" not in st.session_state:
    st.session_state.last_top_df = None
if "last_top_n" not in st.session_state:
    st.session_state.last_top_n = 10

def say(role: str, content: str):
    st.session_state.messages.append({"role": role, "content": content})

def run_script(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr

# ------------------ Chat history ------------------
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# ------------------ Persisted Top Table (IMPORTANT) ------------------
if st.session_state.last_top_df is not None:
    st.subheader(f"Top Jobs (latest top {st.session_state.last_top_n})")
    st.dataframe(st.session_state.last_top_df, use_container_width=True)

# ------------------ Input ------------------
user_msg = st.chat_input("Commands: fetch | match | top 10 | apply 3")

if user_msg:
    say("user", user_msg)
    msg = user_msg.strip()

    # -------- FETCH --------
    if msg.lower() in ["fetch", "fetch jobs", "crawl"]:
        say("assistant", "Running job fetcher (Jobs_Fetch.py)...")
        code, out, err = run_script(["python", "Jobs_Fetch.py"])
        if code == 0:
            say("assistant", f"✅ Fetch complete.\n\n```\n{out[-2000:]}\n```")
        else:
            say("assistant", f"❌ Fetch failed.\n\n```\n{err[-2000:]}\n```")

    # -------- MATCH --------
    elif msg.lower() in ["match", "score", "match jobs"]:
        say("assistant", "Running matcher (Jobs_Matcher.py)...")
        code, out, err = run_script(["python", "Jobs_Matcher.py"])
        if code == 0:
            say("assistant", f"✅ Match complete.\n\n```\n{out[-2000:]}\n```")
        else:
            say("assistant", f"❌ Match failed.\n\n```\n{err[-2000:]}\n```")

    # -------- TOP N --------
    elif msg.lower().startswith("top"):
        parts = msg.split()
        n = 10
        if len(parts) >= 2 and parts[1].isdigit():
            n = int(parts[1])
        st.session_state.last_top_n = n

        if not os.path.exists(PERSONAL_CSV):
            say("assistant", f"Could not find `{PERSONAL_CSV}`. Run `match` first.")
        else:
            df = pd.read_csv(PERSONAL_CSV)
            if "fit_score" in df.columns:
                df["fit_score"] = pd.to_numeric(df["fit_score"], errors="coerce").fillna(0).astype(int)
            else:
                df["fit_score"] = 0

            df = df.sort_values("fit_score", ascending=False).reset_index(drop=True)
            top_df = df.head(n).copy()

            # Store ONLY the columns we want to display
            show_cols = ["company", "title", "location", "fit_score", "visa_sponsor", "verdict", "url"]
            show_cols = [c for c in show_cols if c in top_df.columns]
            st.session_state.last_top_df = top_df[show_cols].copy()

            say("assistant", f"Loaded {len(df)} scored jobs. Showing top {n} in the table below.")

    # -------- APPLY K --------
    elif msg.lower().startswith("apply"):
        parts = msg.split()
        if len(parts) < 2 or not parts[1].isdigit():
            say("assistant", "Usage: `apply 3` (after you run `top 10`).")
        else:
            idx = int(parts[1])

            top_df = st.session_state.last_top_df
            if top_df is None or top_df.empty:
                say("assistant", "No recent `top N` table found. Run `top 10` first.")
            else:
                if idx < 1 or idx > len(top_df):
                    say("assistant", f"Index out of range. Choose 1..{len(top_df)}.")
                else:
                    url = str(top_df.loc[idx - 1, "url"]).strip() if "url" in top_df.columns else ""
                    if not url:
                        say("assistant", "Could not find URL in the selected row.")
                    else:
                        say("assistant", f"Launching auto-apply for #{idx}:\n\n{url}")

                        # IMPORTANT: pass URL as argument!
                        # Auto_Apply.py must read sys.argv[1] to use this.
                        subprocess.Popen(["python", "Auto_Apply.py", url])

                        say("assistant", "✅ Browser launched. It should autofill and pause for review.")

    else:
        say("assistant", "Commands: `fetch`, `match`, `top 10`, `apply 3`")

    # No st.rerun() needed; Streamlit reruns automatically after input
