# smartmoney_nse.py (v1.4) — NSE EOD Smart Money Top 5 with fallback date + logging
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

def ist_today(): return (dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)).date()

def recent_weekdays_from(anchor: dt.date, n=120):
    # generate last n weekdays up to anchor (inclusive), oldest->newest
    out, d = [], anchor
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return out[::-1]

def equity_url(d):
    return f"https://archives.nseindia.com/content/historical/EQUITIES/{d.strftime('%Y')}/{d.strftime('%b').upper()}/cm{d.strftime('%d%b%Y').upper()}bhav.csv.zip"

def fo_url(d):
    return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{d.strftime('%Y')}/{d.strftime('%b').upper()}/fo{d.strftime('%d%b%Y').upper()}bhav.csv.zip"

def get_zip_csv(url):
    # retry + small backoff + log status
    for i in range(4):
        try:
            r = S.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                return pd.read_csv(z.open(z.namelist()[0]))
            print("GET", r.status_code, url)
            time.sleep(1+i)
        except Exception as e:
            print("GET error", url, str(e))
            time.sleep(1+i)
    return None

def get_equity_bhav(d):
    url = equity_url(d)
    df = get_zip_csv(url)
    if df is None: return None
    df = df[df['SERIES'].astype(str).str.upper().eq('EQ')].copy()
    df['DATE'] = d
    for c in ['CLOSE','TOTTRDQTY','TOTTRDVAL']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df[['SYMBOL','CLOSE','TOTTRDQTY','TOTTRDVAL','DATE']]

def get_fo_bhav(d):
    df = get_zip_csv(fo_url(d))
    if df is None: return None
    df['DATE'] = d
    return df

def parse_mto_text(txt):
    lines = [l for l in txt.splitlines() if l.strip()]
    idx = None
    for i,l in enumerate(lines):
        ls = l.lower().replace(' ','')
        if ls.startswith('securityname') or ls.startswith('symbol'):
            idx = i; break
    if idx is None: return None
    data = "\n".join(lines[idx:])
    df = pd.read_csv(io.StringIO(data))
    cols = [c.upper().replace(' ','').replace('(','').replace(')','') for c in df.columns]
    df.columns = cols
    if 'SECURITYNAME' in df.columns and 'SYMBOL' not in df.columns:
        df.rename(columns={'SECURITYNAME':'SYMBOL'}, inplace=True)
    if 'DELIVERABLEQUANTITY' in df.columns:
        df.rename(columns={'DELIVERABLEQUANTITY':'DELIVQTY'}, inplace=True)
    if 'DELIVERABLEQTY' in df.columns and 'DELIVQTY' not in df.columns:
        df.rename(columns={'DELIVERABLEQTY':'DELIVQTY'}, inplace=True)
    if 'TRADESQTY' in df.columns and 'TOTTRDQTY' not in df.columns:
        df.rename(columns={'TRADESQTY':'TOTTRDQTY'}, inplace=True)
    keep = [c for c in ['SYMBOL','DELIVQTY','TOTTRDQTY'] if c in df.columns]
    if not keep: return None
    out = df[keep].copy()
    out['SYMBOL'] = out['SYMBOL'].astype(str).str.upper().str.strip()
    for c in ['DELIVQTY','TOTTRDQTY']:
        if c in out.columns: out[c] = pd.to_numeric(out[c], errors='coerce')
    return out

def get_mto(d):
    url = f"https://archives.nseindia.com/archives/equities/mto/MTO_{d.strftime('%Y%m%d')}.DAT"
    for i in range(4):
        try:
            r = S.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                df = parse_mto_text(r.text)
                if df is None: return None
                df['DATE'] = d
                return df
            print("GET", r.status_code, url)
            time.sleep(1+i)
        except Exception as e:
            print("GET error", url, str(e))
            time.sleep(1+i)
    return None

def nearest_available_date(target, dates):
    ds = sorted(set([x for x in dates if x <= target]))
    return ds[-1] if ds else None

def find_latest_equity_day(max_lookback=7):
    # Find most recent weekday (<= today IST) with available EQUITY bhavcopy
    d = ist_today()
    for i in range(max_lookback+1):
        if d.weekday() < 5:
            df = get_equity_bhav(d)
            if df is not None and not df.empty:
                return d, df
        d -= dt.timedelta(days=1)
    return None, None

def build_eq_hist_upto(eq_date, n_days=90):
    out = []
    for d in recent_weekdays_from(eq_date, n_days+15):
        df = None
        try:
            df = get_equity_bhav(d)
        except Exception:
            df = None
        if df is not None:
            out.append(df)
    if not out:
        return pd.DataFrame(columns=['SYMBOL','CLOSE','TOTTRDQTY','TOTTRDVAL','DATE'])
    return pd.concat(out, ignore_index=True)

def build_mto_hist_upto(eq_date, n=45):
    out = []
    for d in recent_weekdays_from(eq_date, n+15):
        try:
            df = get_mto(d)
            if df is not None: out.append(df)
        except Exception:
            continue
    if not out:
        return pd.DataFrame(columns=['SYMBOL','DELIVQTY','TOTTRDQTY','DATE'])
    return pd.concat(out, ignore_index=True)

def fo_signals_for(eq_date):
    # FO for eq_date and previous available FO day
    df_today = None; today_fo_date = None
    for d in [eq_date] + recent_weekdays_from(eq_date, 10)[::-1]:
        df_today = get_fo_bhav(d)
        if df_today is not None:
            today_fo_date = d; break
    if df_today is None:
        return pd.DataFrame(columns=['SYMBOL','OIChgPct','PxChgPct'])

    df_prev = None
    for d in recent_weekdays_from(today_fo_date - dt.timedelta(days=1), 12)[::-1]:
        df_prev = get_fo_bhav(d)
        if df_prev is not None:
            break
    if df_prev is None:
        return pd.DataFrame(columns=['SYMBOL','OIChgPct','PxChgPct'])

    fut_t = df_today[df_today['INSTRUMENT']=='FUTSTK'].copy()
    fut_p = df_prev[df_prev['INSTRUMENT']=='FUTSTK'].copy()
    if fut_t.empty or fut_p.empty:
        return pd.DataFrame(columns=['SYMBOL','OIChgPct','PxChgPct'])

    fut_t['EXPIRY_DT'] = pd.to_datetime(fut_t['EXPIRY_DT'], format="%d-%b-%Y", errors='coerce')
    fut_p['EXPIRY_DT'] = pd.to_datetime(fut_p['EXPIRY_DT'], format="%d-%b-%Y", errors='coerce')
    fut_t = fut_t.sort_values(['SYMBOL','EXPIRY_DT']).drop_duplicates('SYMBOL', keep='first')
    fut_p = fut_p.sort_values(['SYMBOL','EXPIRY_DT']).drop_duplicates('SYMBOL', keep='first')

    m = fut_t[['SYMBOL','CLOSE','OPEN_INT']].merge(
        fut_p[['SYMBOL','CLOSE','OPEN_INT']].rename(columns={'CLOSE':'CLOSE_PREV','OPEN_INT':'OPEN_INT_PREV'}),
        on='SYMBOL', how='left'
    )
    m['OIChgPct'] = (m['OPEN_INT'] - m['OPEN_INT_PREV'])/m['OPEN_INT_PREV']*100
    m['PxChgPct'] = (m['CLOSE'] - m['CLOSE_PREV'])/m['CLOSE_PREV']*100
    return m[['SYMBOL','OIChgPct','PxChgPct']]

def send_tg(text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN","" ).strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID","" ).strip()
    if not token or not chat:
        print("Telegram creds missing"); return
    try:
        r = S.post(f"https://api.telegram.org/bot{token}/sendMessage",
                   data={"chat_id": chat, "text": text[:3900], "parse_mode":"HTML","disable_web_page_preview":True},
                   timeout=30)
        print("TG:", r.status_code, r.text[:150])
    except Exception as e:
        print("TG error:", e)

def compute_and_send():
    # 1) Find latest available equity day
    eq_date, eq_today_df = find_latest_equity_day(max_lookback=7)
    if eq_date is None:
        send_tg("📭 NSE equity archives not available yet (last 7 days). Will retry next run.")
        return

    # 2) Build history up to that date
    eq = build_eq_hist_upto(eq_date, 90)
    if eq.empty:
        send_tg("📭 Could not build equity history. Will retry next run.")
        return

    eq['DATE'] = pd.to_datetime(eq['DATE']).dt.date
    latest = eq['DATE'].max()
    past = eq[eq['DATE']<latest].copy()
    today = eq[eq['DATE']==latest].copy()

    # 20D avg volume and surge
    past_sorted = past.sort_values(['SYMBOL','DATE'])
    vol20 = past_sorted.groupby('SYMBOL')['TOTTRDQTY'].apply(lambda s: s.tail(20).mean() if s.shape[0]>=10 else np.nan)
    today = today.merge(vol20.rename('VOL20'), left_on='SYMBOL', right_index=True, how='left')
    today['VolSurge'] = today['TOTTRDQTY'] / today['VOL20']

    # Delivery (MTO) nearest <= eq_date
    mto = build_mto_hist_upto(eq_date, 45)
    if not mto.empty:
        mto['DATE'] = pd.to_datetime(mto['DATE']).dt.date
        mto_today_date = nearest_available_date(latest, list(mto['DATE']))
        mto_t = mto[mto['DATE']==mto_today_date].copy()
        if 'TOTTRDQTY' in mto_t.columns and 'DELIVQTY' in mto_t.columns:
            mto_t['DELIVPCT'] = mto_t['DELIVQTY']/mto_t['TOTTRDQTY'].replace(0,np.nan)
        mto_past = mto[mto['DATE']<latest].copy()
        deliv20 = mto_past.sort_values(['SYMBOL','DATE']).groupby('SYMBOL')['DELIVQTY'].apply(lambda s: s.tail(20).mean() if s.shape[0]>=8 else np.nan)
        today = today.merge(mto_t[['SYMBOL','DELIVQTY','DELIVPCT']] if 'DELIVPCT' in mto_t.columns else mto_t[['SYMBOL','DELIVQTY']], on='SYMBOL', how='left')
        today = today.merge(deliv20.rename('DELIV20'), left_on='SYMBOL', right_index=True, how='left')
        today['DelivSurge'] = today['DELIVQTY'] / today['DELIV20']
    else:
        today['DELIVPCT'] = np.nan
        today['DelivSurge'] = np.nan

    # Breakout + RS
    hh = past_sorted.groupby('SYMBOL')['CLOSE'].apply(lambda s: s.tail(55).max() if s.shape[0]>=10 else np.nan)
    today = today.merge(hh.rename('HH55'), left_on='SYMBOL', right_index=True, how='left')
    today['Breakout'] = (today['CLOSE'] >= 0.995*today['HH55']).astype(int)
    med30 = past_sorted.groupby('SYMBOL')['CLOSE'].apply(lambda s: s.tail(30).median() if s.shape[0]>=10 else np.nan)
    today = today.merge(med30.rename('MED30'), left_on='SYMBOL', right_index=True, how='left')
    today['RS'] = today['CLOSE']/today['MED30']

    # F&O long build-up (use FO around eq_date)
    foi = fo_signals_for(latest)
    today = today.merge(foi, on='SYMBOL', how='left')
    today['LongBuildUp'] = ((today['OIChgPct']>3) & (today['PxChgPct']>=-0.1)).astype(int)

    # F&O universe filter
    fno_list = []
    fo_today = get_fo_bhav(latest)
    if fo_today is None:
        # nearest <= latest
        for d in recent_weekdays_from(latest, 5)[::-1]:
            fo_today = get_fo_bhav(d)
            if fo_today is not None: break
    if fo_today is not None:
        fno_list = fo_today[fo_today['INSTRUMENT']=='FUTSTK']['SYMBOL'].dropna().unique().tolist()
        today = today[today['SYMBOL'].isin(fno_list)]

    # Hygiene filters
    today = today[(today['CLOSE']>=100)]
    today = today[today['TOTTRDVAL'] >= 50_00_00_000]  # 50 Cr

    # Score
    def clip_series(s, lo=0, hi=5): return np.clip(s.fillna(0), lo, hi)
    today['Score'] = (
        0.35*clip_series(today['VolSurge'],0,5) +
        0.25*clip_series(today['DelivSurge'],0,5) +
        0.20*today['LongBuildUp'].fillna(0) +
        0.15*today['Breakout'].fillna(0) +
        0.05*clip_series(today['RS'],0,5)
    )

    top5 = today.sort_values('Score', ascending=False).head(5).copy()
    date_str = latest.strftime("%d-%b-%Y")
    if top5.empty:
        send_tg(f"📭 No candidates for {html.escape(date_str)} (data/filters). Will retry next run.")
        return

    lines = [f"<b>India Smart Money Top 5</b> — {html.escape(date_str)}"]
    for i, r in top5.reset_index(drop=True).iterrows():
        sym = html.escape(str(r['SYMBOL']))
        close = f"₹{r['CLOSE']:.2f}"
        vs = "NA" if pd.isna(r['VolSurge']) else f"{r['VolSurge']:.2f}x"
        dp = "NA" if pd.isna(r.get('DELIVPCT')) else f"{r['DELIVPCT']*100:.1f}%"
        ds = "NA" if pd.isna(r.get('DelivSurge
