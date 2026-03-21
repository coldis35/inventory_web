import os
import re
import time
import gspread
import pandas as pd
import streamlit as st
from datetime import date, datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ==========================================
# 1. 페이지 설정
# ==========================================
st.set_page_config(page_title="재고 검색 시스템", page_icon="📦", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&family=JetBrains+Mono:wght@400;600&display=swap');
    html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
    .stApp { background: #0f1117; color: #e0e0e0; }
    .block-container { padding: 1.5rem 2rem 2rem 2rem; max-width: 1400px; }

    .main-header {
        background: linear-gradient(135deg, #1a1f2e 0%, #16213e 50%, #0f3460 100%);
        border: 1px solid #2a3a5c; border-radius: 16px;
        padding: 1.4rem 2rem; margin-bottom: 1.5rem;
    }
    .main-header h1 { font-size: 1.5rem; font-weight: 700; color: #fff; margin: 0; }
    .main-header p  { font-size: 0.82rem; color: #8899bb; margin: 0.2rem 0 0 0; }

    .search-box {
        background: #1a1f2e; border: 1px solid #2a3a5c;
        border-radius: 12px; padding: 1.2rem 1.5rem; margin-bottom: 1rem;
    }
    .stButton > button {
        font-family: 'Noto Sans KR', sans-serif !important;
        font-weight: 600 !important; border-radius: 8px !important;
        border: none !important; transition: all 0.2s !important;
        background: #1e2d45 !important; color: #c0d0e8 !important;
    }
    .stButton > button:hover {
        background: #2a4060 !important; color: #ffffff !important;
        transform: translateY(-1px) !important;
    }
    .kpi-card {
        background: #1a1f2e; border: 1px solid #2a3a5c;
        border-radius: 12px; padding: 1.1rem 1.4rem;
        text-align: center; position: relative; overflow: hidden;
        display: inline-block; min-width: 180px; margin-bottom: 1rem;
    }
    .kpi-card::before {
        content:''; position:absolute; top:0; left:0; right:0;
        height:3px; border-radius:12px 12px 0 0;
    }
    .kpi-inbound::before  { background: #4CAF50; }
    .kpi-outbound::before { background: #FF5722; }
    .kpi-label { font-size:0.72rem; font-weight:500; color:#8899bb;
                 text-transform:uppercase; letter-spacing:0.8px; margin-bottom:0.3rem; }
    .kpi-value { font-family:'JetBrains Mono',monospace; font-size:1.9rem;
                 font-weight:600; line-height:1; margin-bottom:0.2rem; }
    .kpi-sub   { font-size:0.7rem; color:#8899bb; }
    .kpi-inbound  .kpi-value { color: #66BB6A; }
    .kpi-outbound .kpi-value { color: #FF7043; }

    .result-box {
        background: #1a1f2e; border: 1px solid #2a3a5c;
        border-radius: 12px; padding: 1.2rem 1.5rem; margin-top: 1rem;
    }
    .result-title {
        font-size: 0.85rem; font-weight: 600; color: #8899bb;
        text-transform: uppercase; letter-spacing: 1px;
        margin-bottom: 0.8rem; border-bottom: 1px solid #2a3a5c;
        padding-bottom: 0.5rem;
    }
    .info-box {
        background: #1a2a3a; border-left: 3px solid #2196F3;
        border-radius: 0 8px 8px 0; padding: 0.6rem 1rem;
        font-size: 0.82rem; color: #90caf9; margin: 0.4rem 0;
    }
    .warn-box {
        background: #2a1a0a; border-left: 3px solid #FF9800;
        border-radius: 0 8px 8px 0; padding: 0.6rem 1rem;
        font-size: 0.82rem; color: #ffcc02; margin: 0.4rem 0;
    }
    .success-box {
        background: #0a2a0a; border-left: 3px solid #4CAF50;
        border-radius: 0 8px 8px 0; padding: 0.6rem 1rem;
        font-size: 0.82rem; color: #a5d6a7; margin: 0.4rem 0;
    }
    .oos-card {
        background: #1e1a0a; border: 1px solid #5c4a00;
        border-radius: 10px; padding: 1.2rem 1.5rem; margin-top: 0.5rem;
    }
    .oos-row { display:flex; gap:2.5rem; align-items:flex-start; flex-wrap:wrap; }
    .oos-item { display:flex; flex-direction:column; }
    .oos-item-label { font-size:0.7rem; color:#8899bb; text-transform:uppercase;
                      letter-spacing:0.5px; margin-bottom:0.2rem; }
    .oos-item-value { font-family:'JetBrains Mono',monospace; font-size:1rem;
                      font-weight:600; color:#FFA726; }
    .oos-item-text  { font-size:0.9rem; color:#FFA726; }
    label { color: #8899bb !important; font-size: 0.82rem !important; }
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. 구글 인증
# ==========================================
@st.cache_resource
def get_credentials():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    for path in [
        'secrets.json',
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'secrets.json')
    ]:
        try:
            return Credentials.from_service_account_file(path, scopes=scopes)
        except Exception:
            pass
    try:
        return Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=scopes)
    except Exception:
        st.error("secrets.json 또는 웹 금고 설정을 확인하세요.")
        st.stop()

# ==========================================
# 3. 공통 유틸
# ==========================================
def make_unique_columns(cols):
    seen, result = {}, []
    for c in cols:
        c = str(c).strip() or '_unnamed'
        if c in seen:
            seen[c] += 1
            result.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            result.append(c)
    return result

def to_date(val):
    if pd.isna(val) or str(val).strip() == '':
        return None
    s = str(val).strip()[:10]
    for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%Y%m%d']:
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None

def safe_int(val):
    try:
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def kw_match(series, keyword):
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    return series.astype(str).str.lower().str.contains(
        keyword.strip().lower(), na=False, regex=False)

def find_col(cols, exact=None, contains=None, fallback_idx=None):
    if exact:
        for name in (exact if isinstance(exact, list) else [exact]):
            if name in cols:
                return name
    if contains:
        for name in (contains if isinstance(contains, list) else [contains]):
            for c in cols:
                if name in c:
                    return c
    if fallback_idx is not None and fallback_idx < len(cols):
        return cols[fallback_idx]
    return None

# ==========================================
# 4. 데이터 로드 함수
# ==========================================
@st.cache_data(ttl=86400)  # 24시간 캐시
def load_sheet(url, tab_name=None, tab_index=0, header_row=1):
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        doc = gc.open_by_url(url)
        ws = doc.worksheet(tab_name) if tab_name else doc.get_worksheet(tab_index)
        raw = ws.get_all_values()
        if not raw or len(raw) <= header_row:
            return pd.DataFrame()
        headers = make_unique_columns(raw[header_row - 1])
        return pd.DataFrame(raw[header_row:], columns=headers)
    except Exception as e:
        return pd.DataFrame()  # 에러는 호출부에서 처리

@st.cache_data(ttl=86400)  # 24시간 캐시
def get_drive_file_list(folder_id):
    try:
        creds = get_credentials()
        svc = build('drive', 'v3', credentials=creds)
        query = (f"'{folder_id}' in parents and "
                 f"mimeType='application/vnd.google-apps.spreadsheet' and trashed=false")
        items, page_token = [], None
        while True:
            kwargs = dict(q=query, fields="nextPageToken, files(id, name)", pageSize=100)
            if page_token:
                kwargs['pageToken'] = page_token
            res = svc.files().list(**kwargs).execute()
            items.extend(res.get('files', []))
            page_token = res.get('nextPageToken')
            if not page_token:
                break
        return items
    except Exception as e:
        return []  # 에러는 호출부에서 처리

def parse_yyyymm(name):
    # '2022년 출고량 9월' → 202209
    m = re.search(r'(20\d{2})년.*?(\d{1,2})월', name)
    if m:
        return int(m.group(1)) * 100 + int(m.group(2))
    # '202209' 형태
    m = re.search(r'(20\d{4})', name)
    if m:
        return int(m.group(1))
    # '2021 출고량 1-12월' → 연간 파일 → YYYY00
    m = re.search(r'(20\d{2})', name)
    if m:
        return int(m.group(1)) * 100
    return None

SKIP_TABS = ['피벗', '피봇', 'pivot', 'Pivot', '요약', '집계', 'Summary']

def _read_first_data_tab(doc, file_name):
    """파일에서 첫 번째 데이터 탭만 읽어 반환 (중복 방지)"""
    for ws in doc.worksheets():
        if any(s in ws.title for s in SKIP_TABS):
            continue
        try:
            data = ws.get_all_values()
            if data and len(data) > 1:
                headers = make_unique_columns(data[0])
                df = pd.DataFrame(data[1:], columns=headers)
                df['_출처'] = file_name
                return df   # 첫 번째 데이터 탭만 반환
        except Exception:
            pass
    return pd.DataFrame()

@st.cache_data(ttl=604800)  # 과거 파일: 7일 캐시 (변동 없음)
def _load_single_file(file_id, file_name):
    """파일 하나 읽어 캐싱 (file_id가 캐시 키 → 기간 바꿔도 재사용)"""
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(file_id)
        return _read_first_data_tab(doc, file_name)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600)    # 당월 파일: 1시간 캐시
def _load_single_file_current(file_id, file_name):
    """당월 파일 전용 (1시간마다 갱신)"""
    try:
        creds = get_credentials()
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(file_id)
        return _read_first_data_tab(doc, file_name)
    except Exception:
        return pd.DataFrame()

def load_outbound_for_period(folder_id, start_yyyymm, end_yyyymm):
    """기간에 해당하는 파일만 골라 개별 캐시에서 읽어 합산."""
    all_items = get_drive_file_list(folder_id)
    if not all_items:
        return pd.DataFrame()

    current_ym  = date.today().year * 100 + date.today().month
    start_year  = start_yyyymm // 100
    end_year    = end_yyyymm   // 100

    # 기간에 해당하는 파일만 선택
    to_read, skipped = [], 0
    for item in all_items:
        ym    = parse_yyyymm(item['name'])
        if ym is None:
            to_read.append(item)
            continue
        year  = ym // 100
        month = ym % 100
        if month == 0:
            if start_year <= year <= end_year:
                to_read.append(item)
            else:
                skipped += 1
        else:
            if start_yyyymm <= ym <= end_yyyymm:
                to_read.append(item)
            else:
                skipped += 1

    df_list, errors = [], []
    for item in to_read:
        ym = parse_yyyymm(item['name'])
        is_current = (ym is not None and ym % 100 != 0 and ym == current_ym)
        try:
            if is_current:
                df = _load_single_file_current(item['id'], item['name'])
            else:
                df = _load_single_file(item['id'], item['name'])
            if not df.empty:
                df_list.append(df)
        except Exception:
            errors.append(item['name'])

    if errors:
        pass  # 호출부에서 표시

    result_attrs_errors  = errors
    result_attrs_skipped = skipped

    if df_list:
        result = pd.concat(df_list, ignore_index=True, sort=False)
        result.attrs['errors']  = result_attrs_errors
        result.attrs['skipped'] = result_attrs_skipped
        return result
    empty = pd.DataFrame()
    empty.attrs['errors']  = result_attrs_errors
    empty.attrs['skipped'] = result_attrs_skipped
    return empty

# ==========================================
# 5. URL / 폴더 ID
# ==========================================
URL_INBOUND    = "https://docs.google.com/spreadsheets/d/13wwEsR6aXZ01PqfYx6kpPTAiAIpSfp2CeSKqB4WUFhI/edit"
URL_DB_INDEX   = "https://docs.google.com/spreadsheets/d/1M-v7zx8QfOi_NOBfW7V2sKmXwXeETtecDMO2NoZmN3I/edit"  # A열=인덱스번호, M열=상품명
URL_TOTAL_OB   = "https://docs.google.com/spreadsheets/d/1fyeuHQx_mkYIH7ZtK54FLzK9gbQtseDb1URIm-xF3wU/edit"  # 전체 출고량 [2018.05.09~] B=인덱스, C=상품명, D=합계
URL_OUTOFSTOCK = "https://docs.google.com/spreadsheets/d/1mACjH0gb6NYYPHviMAOBuS4JgT6bh4T-2z7-cbP4VjQ/edit"
FOLDER_OUTBOUND_ID = "16qj3-iKIUg9UcKknLkObXU8EvSnFqmnP"

# ==========================================
# 6. 헤더 UI
# ==========================================
st.markdown("""
<div class="main-header">
    <div>
        <h1>📦 입출고 & 재고 검색 시스템</h1>
        <p>입고 내역 · 출고 내역 · 품절 이력 개별 / 통합 조회</p>
    </div>
</div>
""", unsafe_allow_html=True)

# 사이드바: 캐시 초기화
with st.sidebar:
    st.markdown("### ⚙️ 설정")

    # 전체 사전 로드 버튼
    if st.button("📦 출고 데이터 전체 로드\n(오전 1회 실행 권장)", use_container_width=True):
        all_items = get_drive_file_list(FOLDER_OUTBOUND_ID)
        current_ym = date.today().year * 100 + date.today().month
        total = len(all_items)
        ok_count, fail_count = 0, 0
        prog = st.progress(0, text=f"0 / {total} 파일 로드 중...")
        for i, item in enumerate(all_items):
            ym = parse_yyyymm(item['name'])
            is_current = (ym is not None and ym % 100 != 0 and ym == current_ym)
            try:
                if is_current:
                    _load_single_file_current(item['id'], item['name'])
                else:
                    _load_single_file(item['id'], item['name'])
                ok_count += 1
            except Exception:
                fail_count += 1
            import time as _t
            _t.sleep(0.5)   # API 쿼터 보호
            prog.progress((i + 1) / total,
                          text=f"{i+1} / {total} — {item['name']}")
        prog.empty()
        if fail_count:
            st.warning(f"완료: {ok_count}개 성공, {fail_count}개 실패")
        else:
            st.success(f"✅ {ok_count}개 파일 모두 로드 완료! 이제 검색이 빠릅니다.")

    st.markdown("---")

    if st.button("⚡ 당월 출고 새로고침", use_container_width=True):
        _load_single_file_current.clear()
        st.success("당월 출고 데이터 새로고침 완료!")

    if st.button("🔄 전체 캐시 초기화", use_container_width=True):
        st.cache_data.clear()
        st.success("캐시 초기화 완료!")

    st.markdown("---")
    st.caption("📌 캐시 정책")
    st.caption("• 과거 출고 데이터: **7일** 유지")
    st.caption("• 당월 출고 데이터: **1시간** 유지")
    st.caption("• 입고/품절 시트: **24시간** 유지")

# ==========================================
# 7. 검색 입력 폼
# ==========================================
st.markdown('<div class="search-box">', unsafe_allow_html=True)

col_nm, col_idx, col_sd, col_ed = st.columns([2.5, 1.5, 1.2, 1.2])
with col_nm:
    search_name = st.text_input(
        "🔍 상품명 검색",
        placeholder="예: SIMPLIE 수납장 BC264 화이트",
        key="search_name"
    )
with col_idx:
    search_idx = st.text_input(
        "🔢 인덱스 번호 검색",
        placeholder="예: 7313.2106",
        key="search_idx"
    )
with col_sd:
    start_date = st.date_input(
        "📅 출고 조회 시작일",
        value=date(2018, 5, 9),
        min_value=date(2018, 5, 9),
        max_value=date.today(),
        key="start_date"
    )
with col_ed:
    end_date = st.date_input(
        "📅 출고 조회 종료일",
        value=date.today(),
        min_value=date(2018, 5, 9),
        max_value=date.today(),
        key="end_date"
    )

st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

bc1, bc2, bc3, bc4, bc5 = st.columns([2.2, 1.4, 1.4, 1.4, 1.4])
with bc1:
    btn_all      = st.button("🔍  통합 조회  (입고 + 출고 전체 + 품절)", use_container_width=True, key="btn_all")
with bc2:
    btn_inbound  = st.button("📥  입고 내역 조회", use_container_width=True, key="btn_inbound")
with bc3:
    btn_total_ob = st.button("📊  전체 출고량 조회", use_container_width=True, key="btn_total_ob")
with bc4:
    btn_outbound = st.button("📤  기간별 출고 조회", use_container_width=True, key="btn_outbound")
with bc5:
    btn_oos      = st.button("🔴  품절 이력 조회", use_container_width=True, key="btn_oos")

st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# 8. 실행 판단 + DB인덱스 자동 연동
# ==========================================
do_inbound   = btn_all or btn_inbound
do_total_ob  = btn_all or btn_total_ob
do_outbound  = btn_outbound
do_oos       = btn_all or btn_oos

if not any([do_inbound, do_total_ob, do_outbound, do_oos]):
    st.markdown('<div class="info-box">💡 상품명 또는 인덱스 번호 중 하나 이상 입력 후 조회하세요.<br>'
                '📊 전체 출고량: 2018.05.09~현재 합산 (빠름) &nbsp;·&nbsp; 📤 기간별 출고: 드라이브 파일 조회</div>',
                unsafe_allow_html=True)
    st.stop()

if not search_name.strip() and not search_idx.strip():
    st.markdown('<div class="warn-box">⚠️ 상품명 또는 인덱스 번호를 입력해주세요.</div>', unsafe_allow_html=True)
    st.stop()

resolved_name = search_name.strip()
resolved_idx  = search_idx.strip()
matched_index_nos = set()
matched_names     = []

# DB인덱스에서 상품명 <-> 인덱스 자동 연동
with st.spinner("🗂️ DB인덱스 조회 중..."):
    df_dbidx = load_sheet(URL_DB_INDEX, tab_name="index")

if not df_dbidx.empty:
    db_cols     = df_dbidx.columns.tolist()
    db_idx_col  = db_cols[0]
    db_name_col = db_cols[12] if len(db_cols) > 12 else db_cols[-1]

    if resolved_idx:
        mask = df_dbidx[db_idx_col].astype(str).str.strip() == resolved_idx
        matched = df_dbidx[mask]
        if not matched.empty:
            matched_index_nos = {resolved_idx}
            matched_names = matched[db_name_col].astype(str).str.strip().tolist()
            if not resolved_name and matched_names:
                resolved_name = matched_names[0]
                st.markdown(f'<div class="success-box">✅ 인덱스 <strong>{resolved_idx}</strong> → 상품명: <strong>{resolved_name}</strong></div>',
                            unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="warn-box">⚠️ 인덱스 <strong>{resolved_idx}</strong>가 DB인덱스에 없습니다.</div>',
                        unsafe_allow_html=True)

    if resolved_name and not matched_index_nos:
        name_lower = resolved_name.lower()
        mask = df_dbidx[db_name_col].astype(str).str.strip().str.lower() == name_lower
        matched = df_dbidx[mask]
        if not matched.empty:
            matched_index_nos = set(matched[db_idx_col].astype(str).str.strip().tolist())
            matched_names = matched[db_name_col].astype(str).str.strip().tolist()
            idx_display = ", ".join(sorted(matched_index_nos)[:5])
            st.markdown(f'<div class="success-box">✅ 상품명 <strong>{resolved_name}</strong> → 인덱스: <strong>{idx_display}</strong></div>',
                        unsafe_allow_html=True)

# 표시용 검색어
display_kw = resolved_idx or resolved_name

def exact_match(df, name_col, idx_col=None):
    mask = pd.Series([False] * len(df), index=df.index)
    if idx_col and idx_col in df.columns and matched_index_nos:
        col = df[idx_col]
        if isinstance(col, pd.DataFrame): col = col.iloc[:, 0]
        mask |= col.astype(str).str.strip().isin(matched_index_nos)
    if name_col and name_col in df.columns and matched_names:
        col = df[name_col]
        if isinstance(col, pd.DataFrame): col = col.iloc[:, 0]
        for nm in matched_names:
            mask |= col.astype(str).str.strip().str.lower() == nm.lower()
    if not matched_index_nos and not matched_names:
        kw = (resolved_idx or resolved_name).lower()
        for col_candidate in [c for c in [name_col, idx_col] if c and c in df.columns]:
            col = df[col_candidate]
            if isinstance(col, pd.DataFrame): col = col.iloc[:, 0]
            mask |= col.astype(str).str.strip().str.lower() == kw
    return df[mask].copy()

# ==========================================
# 9. 입고 내역 조회
# ==========================================
if do_inbound:
    st.markdown('<div class="result-box">', unsafe_allow_html=True)
    st.markdown('<div class="result-title">📥 입고 내역 (전체 기간 · 샘플 제외)</div>', unsafe_allow_html=True)

    with st.spinner("📥 입고 데이터 로딩 중..."):
        df_ib = load_sheet(URL_INBOUND, tab_name="DB 컨테이너 입고리스트", header_row=3)

    if df_ib.empty:
        st.markdown('<div class="warn-box">⚠️ 입고 데이터를 불러오지 못했습니다. '
                    '"DB 컨테이너 입고리스트" 탭 이름과 공유 권한을 확인하세요.</div>',
                    unsafe_allow_html=True)
    else:
        cols = df_ib.columns.tolist()

        # 컬럼 감지 (exact 우선, 없으면 contains)
        vendor_col = find_col(cols, exact='업체',       contains='업체')
        date_col   = find_col(cols, exact='창고입고일', contains=['입고일', '입항일'])
        loc_col    = find_col(cols, exact='위치',       contains='위치')
        stock_col  = find_col(cols, contains=['품절', '재고현황', '재고'])
        code_col   = find_col(cols, exact='상품코드',   contains='상품코드')
        name_col   = find_col(cols, exact='상품',       contains=['상품명', '상품', '품명'])
        idx_col    = find_col(cols, exact='인덱스번호', contains='인덱스')
        qty_col    = find_col(cols, exact='수량',       contains='수량')
        # qty는 단가/총액 제외
        if qty_col and ('단가' in qty_col or '총액' in qty_col):
            qty_col = next((c for c in cols if '수량' in c
                           and '단가' not in c and '총액' not in c), None)

        # 상품코드 컬럼 (Z열 = 인덱스 기준 25번째 → cols[25])
        code2_col = find_col(cols, exact='상품코드', contains='상품코드')
        # Z열이 실제 상품코드: 컬럼 위치로도 시도
        if not code2_col and len(cols) > 25:
            code2_col = cols[25]

        # 정확히 일치 검색 (DB인덱스 기반)
        df_found = exact_match(df_ib, name_col, idx_col)

        # 샘플 제외
        n_before = len(df_found)
        if code_col and not df_found.empty:
            df_found = df_found[~df_found[code_col].astype(str).str.contains('샘플', na=False)]
        n_removed = n_before - len(df_found)

        if df_found.empty:
            st.markdown(f'<div class="warn-box">⚠️ "{display_kw}" 입고 내역이 없습니다.</div>', unsafe_allow_html=True)
        else:
            total_qty = df_found[qty_col].apply(safe_int).sum() if qty_col else len(df_found)

            col_kpi, col_info = st.columns([1, 3])
            with col_kpi:
                st.markdown(f"""
                <div class="kpi-card kpi-inbound">
                    <div class="kpi-label">총 입고수량</div>
                    <div class="kpi-value">{total_qty:,}</div>
                    <div class="kpi-sub">{len(df_found):,}건{"  ·  샘플 " + str(n_removed) + "건 제외" if n_removed else ""}</div>
                </div>""", unsafe_allow_html=True)
            with col_info:
                if n_removed:
                    st.markdown(f'<div class="info-box">ℹ️ 샘플 항목 {n_removed}건 제외됨</div>',
                                unsafe_allow_html=True)

            # 표시 컬럼: 업체명, 창고입고일, 위치, 품절/재고현황, 상품코드(Z열), 상품, 인덱스번호, 수량
            show_cols = [c for c in [vendor_col, date_col, loc_col, stock_col,
                                     code2_col, name_col, idx_col, qty_col] if c]
            # 중복 제거
            show_cols = list(dict.fromkeys(show_cols))
            if not show_cols:
                show_cols = [c for c in cols if not c.startswith('_')]

            st.dataframe(df_found[show_cols], use_container_width=True,
                         hide_index=True, height=420)

    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# 10. 전체 출고량 조회 (단일 시트 - 빠름)
# ==========================================
if do_total_ob:
    st.markdown('<div class="result-box">', unsafe_allow_html=True)
    st.markdown('<div class="result-title">📊 전체 출고량 (2018.05.09 ~ 현재)</div>', unsafe_allow_html=True)

    with st.spinner("📊 전체 출고량 시트 로딩 중..."):
        df_tob = load_sheet(URL_TOTAL_OB, tab_index=0, header_row=2)

    if df_tob.empty:
        st.markdown('<div class="warn-box">⚠️ 전체 출고량 데이터를 불러오지 못했습니다.</div>', unsafe_allow_html=True)
    else:
        cols = df_tob.columns.tolist()
        # B=인덱스번호(1), C=상품명(2), D=합계(3), E~=연도별
        tob_idx_col  = cols[1] if len(cols) > 1 else None   # B열
        tob_name_col = cols[2] if len(cols) > 2 else None   # C열
        tob_sum_col  = cols[3] if len(cols) > 3 else None   # D열 (합계)

        # 정확히 일치 검색
        df_tob_found = exact_match(df_tob, tob_name_col, tob_idx_col)

        if df_tob_found.empty:
            st.markdown(f'<div class="warn-box">⚠️ "{display_kw}" 전체 출고량 내역이 없습니다.</div>', unsafe_allow_html=True)
        else:
            # 합계 컬럼 합산
            total_tob = df_tob_found[tob_sum_col].apply(safe_int).sum() if tob_sum_col else 0

            col_kpi, _ = st.columns([1, 3])
            with col_kpi:
                st.markdown(f"""
                <div class="kpi-card kpi-outbound">
                    <div class="kpi-label">전체 출고 합계</div>
                    <div class="kpi-value">{total_tob:,}</div>
                    <div class="kpi-sub">2018.05.09 ~ 현재 · {len(df_tob_found)}개 SKU</div>
                </div>""", unsafe_allow_html=True)

            # _unnamed 첫 열 제거 후 표시
            show_cols = [c for c in df_tob_found.columns
                         if not c.startswith('_unnamed') and c != '자동업데이트']
            df_tob_display = df_tob_found[show_cols].copy()

            # 연도 컬럼명 rename: 계→2026, 계_1→2025, ..., 계_8→2018
            year_map = {
                '계':   '2026', '계_1': '2025', '계_2': '2024',
                '계_3': '2023', '계_4': '2022', '계_5': '2021',
                '계_6': '2020', '계_7': '2019', '계_8': '2018',
            }
            df_tob_display = df_tob_display.rename(columns=year_map)
            st.dataframe(df_tob_display, use_container_width=True, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# 11. 기간별 출고 내역 조회 (드라이브 폴더)
# ==========================================
if do_outbound:
    st.markdown('<div class="result-box">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="result-title">📤 출고 내역'
        f'<span style="font-size:0.78rem;font-weight:400;color:#8899bb;margin-left:0.8rem;">'
        f'{start_date.strftime("%Y.%m.%d")} ~ {end_date.strftime("%Y.%m.%d")}</span></div>',
        unsafe_allow_html=True)

    if start_date > end_date:
        st.markdown('<div class="warn-box">⚠️ 시작일이 종료일보다 늦습니다.</div>', unsafe_allow_html=True)
    else:
        start_ym = start_date.year * 100 + start_date.month
        end_ym   = end_date.year   * 100 + end_date.month

        with st.spinner(f"📤 출고 데이터 로딩 중 ({start_date.strftime('%Y.%m')} ~ {end_date.strftime('%Y.%m')})..."):
            df_ob = load_outbound_for_period(FOLDER_OUTBOUND_ID, start_ym, end_ym)

        # 캐시 함수 밖에서 UI 메시지 표시
        _skipped = getattr(df_ob, 'attrs', {}).get('skipped', 0)
        _errors  = getattr(df_ob, 'attrs', {}).get('errors', [])
        if _skipped:
            st.toast(f"⚡ 기간 외 파일 {_skipped}개 건너뜀")
        if _errors:
            st.warning(f"읽기 실패 {len(_errors)}개: {', '.join(_errors[:5])}")

        if df_ob.empty:
            st.markdown('<div class="warn-box">⚠️ 출고 데이터를 불러오지 못했습니다.</div>', unsafe_allow_html=True)
        else:
            cols = df_ob.columns.tolist()
            ob_date_col = find_col(cols, exact=['일시', '날짜'], contains=['일시', '날짜', 'date'])
            ob_idx_col  = find_col(cols, exact=['인덱스', '코드'], contains=['인덱스', '코드'])
            ob_name_col = find_col(cols, exact=['제품명', '상품명', '상품'],
                                   contains=['제품명', '상품명', '상품'])
            ob_qty_col  = find_col(cols, exact='출고량', contains=['출고량', '출고', '수량'])

            # 날짜 필터
            if ob_date_col:
                df_ob['_date'] = df_ob[ob_date_col].apply(to_date)
                df_ob = df_ob[df_ob['_date'].notna()]
                df_ob = df_ob[(df_ob['_date'] >= start_date) & (df_ob['_date'] <= end_date)]

            # 정확히 일치 검색
            df_ob_found = exact_match(df_ob, ob_name_col, ob_idx_col)

            if df_ob_found.empty:
                st.markdown(f'<div class="warn-box">⚠️ "{display_kw}" 출고 내역이 없습니다.</div>',
                            unsafe_allow_html=True)
            else:
                total_ob = df_ob_found[ob_qty_col].apply(safe_int).sum() if ob_qty_col else len(df_ob_found)

                col_kpi, _ = st.columns([1, 3])
                with col_kpi:
                    st.markdown(f"""
                    <div class="kpi-card kpi-outbound">
                        <div class="kpi-label">총 출고수량</div>
                        <div class="kpi-value">{total_ob:,}</div>
                        <div class="kpi-sub">{len(df_ob_found):,}건</div>
                    </div>""", unsafe_allow_html=True)

                show_cols = [c for c in [ob_date_col, ob_idx_col, ob_name_col, ob_qty_col] if c]
                extra = [c for c in df_ob_found.columns
                         if c not in show_cols and not c.startswith('_') and c != '_출처']
                df_display = df_ob_found[show_cols + extra].copy()
                st.dataframe(df_display, use_container_width=True, hide_index=True, height=420)

    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# 11. 품절 이력 조회
# ==========================================
if do_oos:
    st.markdown('<div class="result-box">', unsafe_allow_html=True)
    st.markdown('<div class="result-title">🔴 품절 이력 (가장 최근 품절일 + 입고일)</div>',
                unsafe_allow_html=True)

    with st.spinner("🔴 품절 이력 로딩 중..."):
        # QUERY연도별 탭: A=인덱스, E=상품명, G=공지일자(품절일), H=사유, I=입고일
        df_oos = load_sheet(URL_OUTOFSTOCK, tab_name="QUERY연도별")

    if df_oos.empty:
        st.markdown('<div class="warn-box">⚠️ 품절 이력 데이터를 불러오지 못했습니다. '
                    'QUERY연도별 탭을 확인하세요.</div>', unsafe_allow_html=True)
    else:
        oos_cols = df_oos.columns.tolist()

        # 컬럼: A(0)=인덱스, E(4)=상품명, G(6)=공지일자, H(7)=사유, I(8)=입고일
        oos_idx_col    = find_col(oos_cols, exact=['인덱스', '인덱스번호'],    fallback_idx=0)
        oos_name_col   = find_col(oos_cols, exact=['상품명', '제품명'],        fallback_idx=4)
        oos_oos_col    = find_col(oos_cols, exact=['공지일자', '공지일', '품절일'], fallback_idx=6)
        oos_reason_col = find_col(oos_cols, exact=['사유', '이유', '공지사유'],  fallback_idx=7)
        oos_back_col   = find_col(oos_cols, exact=['입고일', '재입고일'],       fallback_idx=8)

        # 정확히 일치 검색
        df_oos_found = exact_match(df_oos, oos_name_col, oos_idx_col)

        if df_oos_found.empty:
            st.markdown(f'<div class="info-box">ℹ️ "{display_kw}" 품절 이력이 없습니다.</div>',
                        unsafe_allow_html=True)
        else:
            # 가장 최근 품절일 찾기 (날짜순 정렬 → 마지막 행)
            if oos_oos_col:
                df_oos_found['_oos_date'] = df_oos_found[oos_oos_col].apply(to_date)
                df_valid = df_oos_found[df_oos_found['_oos_date'].notna()].copy()
                if not df_valid.empty:
                    df_valid = df_valid.sort_values('_oos_date')
                    latest_row = df_valid.iloc[-1]
                else:
                    latest_row = df_oos_found.iloc[-1]
            else:
                latest_row = df_oos_found.iloc[-1]

            oos_date_val  = str(latest_row.get(oos_oos_col,  '-')).strip() if oos_oos_col  else '-'
            back_date_val = str(latest_row.get(oos_back_col, '-')).strip() if oos_back_col else '-'
            reason_val    = str(latest_row.get(oos_reason_col, '-')).strip() if oos_reason_col else '-'
            name_val      = str(latest_row.get(oos_name_col, display_kw)).strip() if oos_name_col else display_kw

            # 품절 기간 계산
            d1 = to_date(oos_date_val)
            d2 = to_date(back_date_val)
            if d1 and d2:
                days_str = f"{(d2 - d1).days}일"
            elif d1 and not d2:
                days_str = "재입고 미정"
            else:
                days_str = "-"

            if not back_date_val or back_date_val in ['-', 'nan', '']:
                back_date_val = '미정'

            st.markdown(f"""
            <div class="oos-card">
                <div style="font-size:0.78rem; color:#8899bb; margin-bottom:1rem;">
                    📌 가장 최근 품절 기록 &nbsp;·&nbsp;
                    <strong style="color:#e0e0e0;">{name_val}</strong>
                </div>
                <div class="oos-row">
                    <div class="oos-item">
                        <span class="oos-item-label">🔴 품절일 (공지일자)</span>
                        <span class="oos-item-value">{oos_date_val}</span>
                    </div>
                    <div class="oos-item">
                        <span class="oos-item-label">🟢 재입고일</span>
                        <span class="oos-item-value">{back_date_val}</span>
                    </div>
                    <div class="oos-item">
                        <span class="oos-item-label">📆 품절 기간</span>
                        <span class="oos-item-value">{days_str}</span>
                    </div>
                    <div class="oos-item">
                        <span class="oos-item-label">📝 사유</span>
                        <span class="oos-item-text">{reason_val}</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            with st.expander(f"📋 전체 품절 이력 보기 ({len(df_oos_found)}건)"):
                show_cols = [c for c in [oos_idx_col, oos_name_col,
                                         oos_oos_col, oos_reason_col, oos_back_col] if c and c in df_oos_found.columns]
                if not show_cols:
                    show_cols = [c for c in df_oos_found.columns if not c.startswith('_')]
                df_oos_disp = df_oos_found[show_cols].copy()
                # 컬럼명 rename (unnamed 포함 어떤 이름이든 순서 기반으로 덮어씌움)
                rename_map = {}
                labels = ['인덱스번호', '상품명', '품절일', '사유', '입고일']
                for i, col in enumerate(show_cols):
                    if i < len(labels):
                        rename_map[col] = labels[i]
                df_oos_disp = df_oos_disp.rename(columns=rename_map)
                st.dataframe(df_oos_disp, use_container_width=True, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# 푸터
# ==========================================
st.markdown("""
<div style="text-align:center; padding:2rem 0 1rem 0; color:#4a5568; font-size:0.75rem;">
    재고 검색 시스템 · Google Sheets & Drive 실시간 연동 · 캐시 1시간
</div>
""", unsafe_allow_html=True)