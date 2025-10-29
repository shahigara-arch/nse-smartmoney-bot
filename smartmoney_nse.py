# smartmoney_nse.py (v1.4) — NSE EOD Smart Money Top 5 (robust + retry logic)
import pandas as pd, numpy as np, requests, io, zipfile, datetime as dt, os, html, time

UA = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
S = requests.Session()
S.headers.update(UA)
TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5

def ist_today():
    return (dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)).date()

def recent_weekdays(n=90):
    d = ist_today()
    out = []
    tried = 0
    while len(out) < n and tried < n+60:
        d = d - dt.timedelta(days=1)
        if d.weekday() < 5:
            out.append(d)
        tried += 1
    return out[::-1]

def get_zip_csv(url):
    """Fetch ZIP CSV with exponential backoff retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            r = S.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF ** attempt
                    time.sleep(wait_time)
                    continue
                return None
            z = zipfile.ZipFile(io.BytesIO(r.content))
            return pd.read_csv(z.open(z.namelist()[0]))
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** attempt
                time.sleep(wait_time)
            else:
                print(f"get_zip_csv failed after {MAX_RETRIES} attempts: {e}")
                return None
    return None

def get_equity_bhav(d):
    url = f"https://www.nseindia.com/content/historical/EQUITIES/{d.strftime('%d%b%Y').upper()}/cm{d.strftime('%d%b%Y').lower()}bhav.csv.zip"
    return get_zip_csv(url)

def get_fo_bhav(d):
    url = f"https://www.nseindia.com/content/historical/DERIVATIVES/{d.strftime('%d%b%Y').upper()}/fo{d.strftime('%d%b%Y').lower()}bhav.csv.zip"
    return get_zip_csv(url)

def get_fno_list(d):
    url = f"https://www.nseindia.com/content/nsccl/fno_{d.strftime('%d%b%Y').lower()}list.csv"
    try:
        return pd.read_csv(url, timeout=TIMEOUT)
    except:
        return None

def build_eq_hist(dates):
    """Fail-safe equity history builder with fallback handling."""
    eq_hist = None
    
    for d in dates:
        try:
            df = get_equity_bhav(d)
            if df is not None and not df.empty:
                df['Date'] = d
                if eq_hist is None:
                    eq_hist = df
                else:
                    eq_hist = pd.concat([eq_hist, df], ignore_index=True)
        except Exception as e:
            print(f"Warning: Failed to fetch {d}: {e}")
            continue
    
    if eq_hist is None or eq_hist.empty:
        print("Warning: No equity history data fetched. Using empty DataFrame.")
        return pd.DataFrame(columns=['Date'])
    
    return eq_hist

def build_cm_metrics(today_df, eq_hist):
    """Build commodity metrics with null checks."""
    try:
        if today_df is None or today_df.empty or eq_hist is None or eq_hist.empty:
            return today_df
        
        # Guard against empty DataFrames
        if len(today_df) == 0 or len(eq_hist) == 0:
            return today_df
        
        today_df['VolSurge'] = 0.0
        today_df['DelivSurge'] = 0.0
        
        for sym in today_df['SYMBOL'].unique():
            sym_hist = eq_hist[eq_hist['SYMBOL'] == sym].sort_values('Date')
            if len(sym_hist) >= 20:
                vol_median = sym_hist.tail(20)['TOTTRDVOL'].median()
                deliv_median = (sym_hist.tail(20).get('DELQTY', pd.Series(0)).sum() / sym_hist.tail(20)['TOTTRDVOL'].sum()) if sym_hist.tail(20)['TOTTRDVOL'].sum() > 0 else 0
                
                today_vol = today_df[today_df['SYMBOL'] == sym]['TOTTRDVOL'].values[0]
                today_deliv = today_df[today_df['SYMBOL'] == sym].get('DELQTY', pd.Series(0)).sum() / today_vol if today_vol > 0 else 0
                
                today_df.loc[today_df['SYMBOL'] == sym, 'VolSurge'] = today_vol / vol_median if vol_median > 0 else 1.0
                today_df.loc[today_df['SYMBOL'] == sym, 'DelivSurge'] = today_deliv / deliv_median if deliv_median > 0 else 1.0
        
        return today_df
    except Exception as e:
        print(f"Warning: build_cm_metrics failed: {e}")
        return today_df

def compute_and_send():
    """Main compute function with robust guards and edge case handling."""
    latest = ist_today()
    
    # Guard: Check if we're on a weekend
    if latest.weekday() >= 5:
        send_tg(f"📅 Today ({latest.strftime('%d-%b-%Y')}) is weekend. No market data expected.")
        return
    
    # Fetch today's data
    today = get_equity_bhav(latest)
    if today is None or today.empty:
        send_tg("❌ Failed to fetch today's equity data. Retrying next schedule.")
        return
    
    today['TOTTRDVAL'] = pd.to_numeric(today.get('TOTTRDVAL', 0), errors='coerce').fillna(0)
    today['CLOSE'] = pd.to_numeric(today.get('CLOSE', 0), errors='coerce').fillna(0)
    
    # Guard: Fallback to FNO list if available
    fno_list = []
    fo_today = get_fo_bhav(latest)
    if fo_today is not None and not fo_today.empty:
        fno_list = fo_today[fo_today['INSTRUMENT'] == 'FUTSTK']['SYMBOL'].dropna().unique().tolist()
        if len(fno_list) > 0:
            today = today[today['SYMBOL'].isin(fno_list)]
    
    # Hygiene filters
    today = today[(today['CLOSE'] >= 100)]
    today = today[today['TOTTRDVAL'] >= 50_00_00_000]
    
    if today.empty:
        send_tg(f"📭 No candidates for {latest.strftime('%d-%b-%Y')} (filters strict or data missing).")
        return
    
    # Build metrics with fail-safe fallback
    dates = recent_weekdays(90)
    eq_hist = build_eq_hist(dates)
    today = build_cm_metrics(today, eq_hist)
    
    # Score blend with clipping
    def clip_series(s, lo=0, hi=5):
        return np.clip(s.fillna(0), lo, hi)
    
    today['Score'] = (
        0.35 * clip_series(today['VolSurge'], 0, 5) +
        0.25 * clip_series(today.get('DelivSurge', pd.Series(0)), 0, 5) +
        0.20 * today.get('LongBuildUp', pd.Series(0)).fillna(0) +
        0.15 * today.get('Breakout', pd.Series(0)).fillna(0) +
        0.05 * clip_series(today.get('RS', pd.Series(0)), 0, 5)
    )
    
    top5 = today.sort_values('Score', ascending=False).head(5).copy()
    date_str = latest.strftime("%d-%b-%Y")
    
    if top5.empty:
        send_tg(f"📭 No candidates for {html.escape(date_str)} (filters strict or data missing).")
        return
    
    lines = [f"India Smart Money Top 5 — {html.escape(date_str)}"]
    
    for i, r in top5.reset_index(drop=True).iterrows():
        sym = html.escape(str(r.get('SYMBOL', 'N/A')))
        close = f"₹{r.get('CLOSE', 0):.2f}"
        vs = "NA" if pd.isna(r.get('VolSurge')) else f"{r.get('VolSurge', 0):.2f}x"
        dp = "NA" if pd.isna(r.get('DELIVPCT')) else f"{r.get('DELIVPCT', 0)*100:.1f}%"
        ds = "NA" if pd.isna(r.get('DelivSurge')) else f"{r.get('DelivSurge', 0):.2f}x"
        oi = "NA" if pd.isna(r.get('OIChgPct')) else f"{r.get('OIChgPct', 0):.1f}%"
        bo = "Yes" if int(r.get('Breakout', 0)) == 1 else "No"
        sc = f"{r.get('Score', 0):.2f}"
        
        lines.append(f"{i+1}) <b>{sym}</b> — {close} | Vol:{vs} | Del:{dp} ({ds}) | OIΔ:{oi} | BO:{bo} | Score:{sc}")
    
    send_tg("\n".join(lines))

def send_tg(text):
    """Send message to Telegram with error handling."""
    try:
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        if not token or not chat_id:
            print("Warning: Telegram credentials not set")
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}, timeout=TIMEOUT)
    except Exception as e:
        print(f"Telegram send error: {e}")

if __name__ == "__main__":
    try:
        send_tg("🕗 SmartMoney scan started (NSE EOD)")
        compute_and_send()
    except Exception as e:
        send_tg(f"⚠️ SmartMoney job failed: <code>{html.escape(str(e))}</code>")
        print("Error:", e)
