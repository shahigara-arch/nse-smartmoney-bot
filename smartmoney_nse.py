# smartmoney_nse.py (v1.3) — NSE EOD Smart Money Top 5 (free + robust)
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
    r = S.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        return None
    z = zipfile.ZipFile(io.BytesIO(r.content))
    return pd.read_csv(z.open(z.namelist()[0]))

def get_equity_bhav(d):
    url = f"https://archives.nseindia.com/content/historical/EQUITIES/{d.strftime('%Y')}/{d.strftime('%b').upper()}/cm{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
    df = get_zip_csv(url)
    if df is None: return None
    df = df[df['SERIES'].astype(str).str.upper().eq('EQ')].copy()
    df['DATE'] = d
    # ensure numeric
    for c in ['CLOSE','TOTTRDQTY','TOTTRDVAL']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df[['SYMBOL','CLOSE','TOTTRDQTY','TOTTRDVAL','DATE']]

def get_fo_bhav(d):
    url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{d.strftime('%Y')}/{d.strftime('%b').upper()}/fo{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
    df = get_zip_csv(url)
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
    # MTO may have SECURITYNAME (which is usually the symbol code)
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
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce')
    return out

def get_mto(d):
    url = f"https://archives.nseindia.com/archives/equities/mto/MTO_{d.strftime('%Y%m%d')}.DAT"
    r = S.get(url, timeout=TIMEOUT)
    if r.status_code != 200: return None
    df = parse_mto_text(r.text)
    if df is None: return None
    df['DATE'] = d
    return df

def nearest_available_date(target, dates):
    ds = sorted(set([x for x in dates if x <= target]))
    return ds[-1] if ds else None

def build_eq_hist(n=90):
    out = []
    for d in recent_weekdays(n+15):
        try:
            df = get_equity_bhav(d)
            if df is not None:
                out.append(df)
        except Exception:
            continue
    if not out: raise RuntimeError("Equity bhavcopy missing.")
    return pd.concat(out, ignore_index=True)

def build_mto_hist(n=45):
    out = []
    for d in recent_weekdays(n+15):
        try:
            df = get_mto(d)
            if df is not None:
                out.append(df)
        except Exception:
            continue
    if not out:
        return pd.DataFrame(columns=['SYMBOL','DELIVQTY','TOTTRDQTY','DATE'])
    return pd.concat(out, ignore_index=True)

def fo_signals(ref_date):
    # today (nearest available <= ref_date) and previous FO day
    # find today FO
    df_today = None
    for d in [ref_date] + recent_weekdays(10)[::-1]:
        try:
            df_today = get_fo_bhav(d)
            if df_today is not None:
                today_fo_date = d
                break
        except Exception:
            continue
    if df_today is None:
        return pd.DataFrame(columns=['SYMBOL','OIChgPct','PxChgPct'])

    # find prev FO strictly before today_fo_date
    df_prev = None
    for d in recent_weekdays(12)[-2::-1]:
        if d >= today_fo_date: continue
        try:
            df_prev = get_fo_bhav(d)
            if df_prev is not None:
                break
        except Exception:
            continue
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
    token = os.environ.get("TELEGRAM_BOT_TOKEN","").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat:
        print("Telegram creds missing"); return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = S.post(url, data={"chat_id": chat, "text": text[:3900], "parse_mode":"HTML","disable_web_page_preview":True}, timeout=30)
        print("TG:", r.status_code, r.text[:150])
    except Exception as e:
        print("TG error:", e)

def compute_and_send():
    eq = build_eq_hist(90)
    eq['DATE'] = pd.to_datetime(eq['DATE']).dt.date
    latest = eq['DATE'].max()
    past = eq[eq['DATE']<latest].copy()
    today = eq[eq['DATE']==latest].copy()

    # 20-day avg volume
    past_sorted = past.sort_values(['SYMBOL','DATE'])
    vol20 = past_sorted.groupby('SYMBOL')['TOTTRDQTY'].apply(lambda s: s.tail(20).mean() if s.shape[0]>=10 else np.nan)
    today = today.merge(vol20.rename('VOL20'), left_on='SYMBOL', right_index=True, how='left')
    today['VolSurge'] = today['TOTTRDQTY'] / today['VOL20']

    # Delivery (MTO)
    mto = build_mto_hist(45)
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

    # Breakout near 55-day high
    hh = past_sorted.groupby('SYMBOL')['CLOSE'].apply(lambda s: s.tail(55).max() if s.shape[0]>=10 else np.nan)
    today = today.merge(hh.rename('HH55'), left_on='SYMBOL', right_index=True, how='left')
    today['Breakout'] = (today['CLOSE'] >= 0.995*today['HH55']).astype(int)

    # RS vs 30-day median
    med30 = past_sorted.groupby('SYMBOL')['CLOSE'].apply(lambda s: s.tail(30).median() if s.shape[0]>=10 else np.nan)
    today = today.merge(med30.rename('MED30'), left_on='SYMBOL', right_index=True, how='left')
    today['RS'] = today['CLOSE']/today['MED30']

    # F&O long build-up
    foi = fo_signals(latest)
    today = today.merge(foi, on='SYMBOL', how='left')
    today['LongBuildUp'] = ((today['OIChgPct']>3) & (today['PxChgPct']>=-0.1)).astype(int)

    # F&O universe filter (liquidity)
    fno_list = []
    fo_today = get_fo_bhav(latest)
    if fo_today is None:
        # fallback: find nearest FO day within last 5 weekdays
        for d in recent_weekdays(5)[::-1]:
            fo_today = get_fo_bhav(d)
            if fo_today is not None:
                break
    if fo_today is not None:
        fno_list = fo_today[fo_today['INSTRUMENT']=='FUTSTK']['SYMBOL'].dropna().unique().tolist()
        today = today[today['SYMBOL'].isin(fno_list)]

    # Hygiene filters
    today = today[(today['CLOSE']>=100)]
    # Value traded ≥ 50 Cr (5e8 INR)
    today = today[today['TOTTRDVAL'] >= 50_00_00_000]

    # Score blend
    def clip_series(s, lo=0, hi=5):
        return np.clip(s.fillna(0), lo, hi)

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
        send_tg(f"📭 No candidates for {html.escape(date_str)} (filters strict or data missing).")
        return

    lines = [f"<b>India Smart Money Top 5</b> — {html.escape(date_str)}"]
    for i, r in top5.reset_index(drop=True).iterrows():
        sym = html.escape(str(r['SYMBOL']))
        close = f"₹{r['CLOSE']:.2f}"
        vs = "NA" if pd.isna(r['VolSurge']) else f"{r['VolSurge']:.2f}x"
        dp = "NA" if pd.isna(r.get('DELIVPCT')) else f"{r['DELIVPCT']*100:.1f}%"
        ds = "NA" if pd.isna(r.get('DelivSurge')) else f"{r['DelivSurge']:.2f}x"
        oi = "NA" if pd.isna(r.get('OIChgPct')) else f"{r['OIChgPct']:.1f}%"
        bo = "Yes" if int(r.get('Breakout',0))==1 else "No"
        sc = f"{r['Score']:.2f}"
        lines.append(f"{i+1}) <b>{sym}</b> — {close} | Vol:{vs} | Del:{dp} ({ds}) | OIΔ:{oi} | BO:{bo} | Score:{sc}")
    send_tg("\n".join(lines))

if __name__ == "__main__":
    try:
        send_tg("🕗 SmartMoney scan started (NSE EOD)")
        compute_and_send()
    except Exception as e:
        send_tg(f"⚠️ SmartMoney job failed: <code>{html.escape(str(e))}</code>")
        print("Error:", e)
