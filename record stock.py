import streamlit as st
import pandas as pd
import plotly.express as px
import os
import feedparser
import socket
import twstock
import yfinance as yf
from datetime import timedelta
import urllib.parse
from collections import Counter

# --- 設定頁面 ---
st.set_page_config(page_title="股市風箏紀錄系統", layout="wide")
st.title("🪁 股市風箏 - 每日戰情室 (歷史回溯版)")

# 設定全域 socket timeout
socket.setdefaulttimeout(3.0)

# --- 0. 產業翻譯字典 (核心大腦) ---
# 將 Yahoo Finance 的英文分類翻譯成台灣習慣的細分產業
INDUSTRY_TRANSLATION = {
    # --- PCB / 載板 ---
    "Printed Circuit Boards": "PCB-印刷電路板",
    "Electronic Components": "電子零組件",
    
    # --- 半導體 ---
    "Semiconductors": "半導體製造/IC設計",
    "Semiconductor Equipment & Materials": "半導體設備&材料",
    
    # --- 電腦與周邊 ---
    "Computer Hardware": "電腦硬體/伺服器",
    "Consumer Electronics": "消費電子",
    "Communication Equipment": "網通設備",
    "Computer Systems": "電腦系統/系統整合",
    
    # --- 傳產/其他 ---
    "Auto Parts": "汽車零組件",
    "Specialty Chemicals": "特用化學",
    "Electrical Equipment & Parts": "電機機械",
    "Farm & Heavy Construction Machinery": "重電/機械",
    "Engineering & Construction": "工程營造",
    "Marine Shipping": "航運",
    "Aerospace & Defense": "航太軍工",
    "Solar": "太陽能",
    "Packaging & Containers": "包材",
}

# --- 1. 股票核心功能 ---
@st.cache_resource
def get_stock_map():
    """建立基礎對照表 (包含部分手動修正)"""
    name_to_info = {}
    
    # 1. 載入標準資料
    for code, info in twstock.codes.items():
        if info.type in ['股票', 'ETF']:
            suffix = '.TW' if info.market == '上市' else '.TWO'
            industry = info.group if info.group else info.type
            name_to_info[info.name] = {
                'code': code,
                'ticker': f"{code}{suffix}",
                'industry': industry, # 這是官方大分類，稍後會被覆蓋
                'market': info.market
            }
            
    # 2. 手動強力修正 (針對 KY 股或特殊股)
    manual_fixes = {
        "IET-KY": {"code": "4971", "market": "上櫃"},
        "ITE-KY": {"code": "4971", "market": "上櫃"}, 
        "AES-KY": {"code": "6781", "market": "上市"},
        "jpp-KY": {"code": "5284", "market": "上櫃"},
        "世芯-KY": {"code": "3661", "market": "上市"},
        "矽力*-KY": {"code": "6415", "market": "上市"},
        "譜瑞-KY": {"code": "4966", "market": "上櫃"},
    }
    
    for name, data in manual_fixes.items():
        suffix = '.TW' if data['market'] == '上市' else '.TWO'
        # 這裡先給個預設值，詳細產業稍後由自動偵測填補
        name_to_info[name] = {
            'code': data['code'],
            'ticker': f"{data['code']}{suffix}",
            'industry': "其他電子", 
            'market': data['market']
        }
            
    return name_to_info

@st.cache_data(ttl=86400) # 產業資訊快取 1 天 (因為不會常變)
def fetch_detailed_industry_batch(stock_list):
    """
    【自動化產業偵測】
    批次去問 Yahoo Finance 這些股票的細分產業 (Industry)
    """
    if not stock_list: return {}
    
    stock_map = get_stock_map()
    ticker_to_name = {}
    tickers = []
    
    for name in stock_list:
        clean = str(name).strip()
        info = stock_map.get(clean)
        if info:
            tickers.append(info['ticker'])
            ticker_to_name[info['ticker']] = clean
            
    if not tickers: return {}
    
    industry_map = {}
    
    # yfinance 的 info 抓取比較慢，我們用 Ticker 物件逐一抓取
    # 為了效能，這裡只抓取必要的 info
    for ticker in tickers:
        try:
            # 使用 yfinance 抓取詳細資料
            yf_stock = yf.Ticker(ticker)
            # 取得英文產業名稱
            eng_industry = yf_stock.info.get('industry', '')
            
            if eng_industry:
                # 翻譯成中文
                tw_industry = INDUSTRY_TRANSLATION.get(eng_industry, eng_industry)
                # 如果翻譯不到，就保留英文原名或做簡單處理
                if tw_industry == eng_industry:
                     # 簡單的翻譯嘗試 (取代常見字)
                     tw_industry = tw_industry.replace("Equipment", "設備").replace("Parts", "零組件").replace("Services", "服務")
                
                stock_name = ticker_to_name.get(ticker)
                industry_map[stock_name] = tw_industry
        except:
            continue
            
    return industry_map

@st.cache_data(ttl=600)
def get_historical_data(stock_names_list, target_date_str):
    """【時光機】抓取指定日期的歷史股價"""
    if not stock_names_list: return {}
    
    progress_text = "正在連線報價伺服器..."
    my_bar = st.progress(0, text=progress_text)
    
    try:
        stock_map = get_stock_map()
        tickers_to_fetch = []
        ticker_to_name = {}
        
        for name in stock_names_list:
            clean_name = str(name).strip()
            info = stock_map.get(clean_name)
            if info:
                ticker = info['ticker']
                tickers_to_fetch.append(ticker)
                ticker_to_name[ticker] = clean_name
                
        tickers_to_fetch = list(set(tickers_to_fetch))
        if not tickers_to_fetch: 
            my_bar.empty()
            return {}

        results = {}
        start_date = pd.to_datetime(target_date_str)
        end_date = start_date + timedelta(days=1)
        
        my_bar.progress(30, text="正在發送請求 (限時 5 秒)...")
        
        try:
            data = yf.download(tickers_to_fetch, start=start_date, end=end_date, 
                               group_by='ticker', progress=False, threads=False, timeout=5)
        except Exception:
            my_bar.empty()
            return {}
        
        my_bar.progress(60, text="正在解析資料...")
        
        # === V27.0: 順便觸發自動產業偵測 (如果還沒快取過) ===
        # 為了不讓畫面卡太久，我們只對「沒有詳細資料」的股票做偵測
        # 但為了流暢度，這裡先用基礎資料，詳細產業在月度統計時才顯示
        
        if data.empty:
            my_bar.empty()
            return {}

        for ticker in tickers_to_fetch:
            try:
                if len(tickers_to_fetch) == 1:
                    df_stock = data
                    if 'Close' not in df_stock.columns: continue
                else: 
                    if ticker not in data.columns.levels[0]: continue
                    df_stock = data[ticker]
                
                if not df_stock.empty:
                    day_record = df_stock.iloc[0]
                    close_price = day_record['Close']
                    volume = day_record['Volume']
                    
                    if pd.notnull(close_price) and pd.notnull(volume) and volume > 0:
                        amount = close_price * volume
                        if amount > 100000000: amt_str = f"{amount/100000000:.1f}億"
                        else: amt_str = f"{amount/10000:.0f}萬"
                            
                        stock_name = ticker_to_name.get(ticker)
                        stock_info = stock_map.get(stock_name)
                        
                        # 預設使用官方大分類
                        industry = stock_info['industry'] if stock_info else "其他"
                        
                        real_code = ticker.split('.')[0]
                        results[stock_name] = {
                            "code": real_code,
                            "industry": industry,
                            "price": close_price,
                            "amount_str": amt_str,
                            "vol_str": f"{volume/1000:.0f}張"
                        }
            except: continue
            
        my_bar.empty()
        return results

    except Exception as e:
        print(f"Historical fetch error: {e}")
        my_bar.empty()
        return {}

# --- 資料讀取 ---
@st.cache_data(ttl=60)
def load_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    excel_filename = '風箏.xlsx'
    file_path = os.path.join(current_dir, excel_filename)

    try:
        df_headers = pd.read_excel(file_path, header=None, nrows=3, engine='openpyxl')
        df = pd.read_excel(file_path, header=None, skiprows=3, engine='openpyxl')
        
        new_columns = []
        last_cat, last_sub = "", ""
        
        for i in range(df_headers.shape[1]):
            r0 = str(df_headers.iloc[0, i]).strip().replace('\n', '').replace('\r', '')
            r1 = str(df_headers.iloc[1, i]).strip().replace('\n', '').replace('\r', '')
            r2 = str(df_headers.iloc[2, i]).strip().replace('\n', '').replace('\r', '')
            
            if r0 == 'nan': r0 = last_cat
            else: last_cat = r0
            if r1 == 'nan': r1 = last_sub
            else: last_sub = r1
            
            if r2.replace('.0', '').isdigit():
                col_name = f"{r0}_{r1}_TOP{int(float(r2))}"
            else:
                col_name = r1 if r1 != 'nan' else r0
            new_columns.append(col_name)
            
        df.columns = new_columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        date_col = next((c for c in df.columns if '日期' in c), None)
        if date_col:
            df = df.rename(columns={date_col: '日期'})
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
            df = df.dropna(subset=['日期'])
            df = df.sort_values('日期')
        else:
            return None
        return df
    except Exception:
        return None

# --- 新聞抓取 ---
@st.cache_data(ttl=600)
def fetch_specific_stock_news(stock_list, target_date_str):
    if not stock_list: return []
    try:
        socket.setdefaulttimeout(3.0) 
        search_keywords = list(set(stock_list))[:15]
        stocks_str = " OR ".join(search_keywords)
        source_str = "(鉅亨網 OR cnyes)"
        
        target_dt = pd.to_datetime(target_date_str)
        end_dt = target_dt + timedelta(days=1)
        start_dt = target_dt - timedelta(days=30)
        
        date_query = f"after:{start_dt.strftime('%Y-%m-%d')} before:{end_dt.strftime('%Y-%m-%d')}"
        query = f"({stocks_str}) AND {source_str} {date_query}"
        
        encoded_query = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        
        feed = feedparser.parse(rss_url)
        return feed.entries[:20]
    except Exception:
        return []

# --- 顯示輔助 ---
def display_stock_list_with_data(day_data, title, category_key, sub_key, stock_info_dict, top_n=3, color="blue", detailed_map=None):
    st.markdown(f"##### {title}")
    
    relevant_cols = [c for c in day_data.index if category_key in c and sub_key in c and "TOP" in c]
    relevant_cols.sort(key=lambda x: int(x.split('TOP')[-1]))
    
    found_any = False
    for col in relevant_cols[:top_n]:
        stock_name = day_data[col]
        if pd.notnull(stock_name) and str(stock_name) != 'nan':
            rank = col.split('TOP')[-1]
            stock_name = str(stock_name).strip()
            
            info_str = ""
            if stock_name in stock_info_dict:
                info = stock_info_dict[stock_name]
                
                # === V27.0 使用自動偵測的產業名稱 (如果有) ===
                show_industry = info['industry']
                if detailed_map and stock_name in detailed_map:
                    show_industry = detailed_map[stock_name]
                
                info_str = f"<span style='font-size:0.8em; color:#555; background-color:#f0f2f6; padding:2px 4px; border-radius:4px;'>({show_industry} | {info['code']} | ${info['price']:.1f} | 💰{info['amount_str']})</span>"
            else:
                pass
            
            st.markdown(f"- <span style='color:{color}; font-weight:bold'>T{rank}</span> : **{stock_name}** {info_str}", unsafe_allow_html=True)
            found_any = True
            
    if not found_any:
        st.caption("無資料")

def make_highlighter(color):
    def highlighter(val):
        try:
            if pd.notnull(val) and isinstance(val, (int, float)) and val > 30:
                return f'background-color: {color}; color: black; font-weight: bold'
        except: pass
        return ''
    return highlighter

# --- 策略分組+產業歸納 ---
def calculate_monthly_strategy_grouped(df, target_date_str, stock_map, detailed_map):
    target_date = pd.to_datetime(target_date_str)
    mask = (df['日期'].dt.year == target_date.year) & \
           (df['日期'].dt.month == target_date.month) & \
           (df['日期'] <= target_date)
    month_df = df[mask]
    
    if month_df.empty: return {}
    
    strat_tree = {}
    strategy_cols = [c for c in month_df.columns if "TOP" in c]
    
    for _, row in month_df.iterrows():
        for col in strategy_cols:
            stock_name = row[col]
            if pd.notnull(stock_name) and str(stock_name) != 'nan':
                stock_name = str(stock_name).strip()
                parts = col.split('_')
                if len(parts) >= 2:
                    raw_strat = parts[1] if len(parts) >= 3 else parts[0]
                    strat_name = raw_strat.replace("TOP", "").replace("排名", "")
                    
                    # === V27.0 優先使用自動偵測的細分產業 ===
                    industry = "其他"
                    if detailed_map and stock_name in detailed_map:
                        industry = detailed_map[stock_name]
                    else:
                        info = stock_map.get(stock_name)
                        industry = info['industry'] if info else "其他"
                    
                    if strat_name not in strat_tree:
                        strat_tree[strat_name] = {}
                    if industry not in strat_tree[strat_name]:
                        strat_tree[strat_name][industry] = Counter()
                    strat_tree[strat_name][industry][stock_name] += 1
    return strat_tree

# === 主程式 ===
df = load_data()
stock_map_global = get_stock_map()

if df is not None:
    tab1, tab2 = st.tabs(["📊 每日個股戰情室", "📈 長期策略趨勢"])

    with tab1:
        st.header("每日個股清單與盤勢 (歷史紀錄)")
        st.caption("💡 括號內資訊為該日期的：(產業 | 代碼 | 收盤價 | 成交金額)")
        
        try:
            date_list = df['日期'].dt.strftime('%Y-%m-%d').unique().tolist()
            col_sel, col_info = st.columns([1, 3])
            with col_sel:
                selected_date_str = st.selectbox("📅 請選擇日期:", date_list, index=len(date_list)-1)
            
            day_data_df = df[df['日期'] == selected_date_str]
            
            if not day_data_df.empty:
                day_data = day_data_df.iloc[0]
                
                # 1. 找出需要抓取資料的股票清單
                all_target_stocks = []
                target_keys = ["上班族", "老闆", "TOP30"]
                for col_name in day_data.index:
                    if any(k in col_name for k in target_keys) and "TOP" in col_name:
                        val = day_data[col_name]
                        if pd.notnull(val) and str(val) != 'nan':
                            all_target_stocks.append(str(val).strip())
                
                unique_stocks = list(set(all_target_stocks))
                
                stock_info_dict = {}
                related_news = []
                detailed_industry_map = {} # 存放自動偵測的產業
                
                if unique_stocks:
                    with st.status("正在連線財經資料庫...", expanded=True) as status:
                        # 步驟 1: 抓股價
                        st.write("🔍 正在查詢歷史股價 (Yahoo Finance)...")
                        stock_info_dict = get_historical_data(unique_stocks, selected_date_str)
                        
                        # 步驟 2: 抓詳細產業 (V27.0 新增)
                        # 這一步會去 Yahoo 爬詳細資料，只針對我們這個月有出現的股票
                        st.write("🏭 正在分析產業細分類...")
                        # 為了統計完整性，我們應該把「這個月所有出現過的股票」都拿去查
                        # 但為了效能，目前先只查「今天」的，或是累計表裡面的
                        detailed_industry_map = fetch_detailed_industry_batch(unique_stocks)
                        
                        # 步驟 3: 抓新聞
                        st.write("📰 正在搜尋相關新聞 (鉅亨網)...")
                        related_news = fetch_specific_stock_news(unique_stocks, selected_date_str)
                        
                        status.update(label="✅ 資料載入完成！", state="complete", expanded=False)

                wind_col = next((c for c in df.columns if '風度' in c), None)
                if wind_col:
                    wind_val = day_data.get(wind_col, '未知')
                    wind_color = "gray"
                    if "強風" in str(wind_val): wind_color = "#d32f2f"
                    elif "亂流" in str(wind_val): wind_color = "#f57c00"
                    elif "無風" in str(wind_val): wind_color = "#1976d2"
                    with col_info:
                        st.markdown(f"### 當日風向：<span style='color:{wind_color}'>{wind_val}</span>", unsafe_allow_html=True)

                st.divider()

                # 顯示列表 (傳入 detailed_map)
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.info("🏢 **上班族型**")
                    display_stock_list_with_data(day_data, "🔥 強勢周 (前3名)", "上班族", "強勢周", stock_info_dict, 3, "#1565c0", detailed_industry_map)
                    st.write("")
                    display_stock_list_with_data(day_data, "📈 周趨勢 (前3名)", "上班族", "周趨勢", stock_info_dict, 3, "#1565c0", detailed_industry_map)
                with c2:
                    st.warning("👑 **老闆型**")
                    display_stock_list_with_data(day_data, "📉 周拉回 (前3名)", "老闆", "周拉回", stock_info_dict, 3, "#e65100", detailed_industry_map)
                    st.write("")
                    display_stock_list_with_data(day_data, "💰 廉價收購 (前3名)", "老闆", "廉價收購", stock_info_dict, 3, "#e65100", detailed_industry_map)
                with c3:
                    st.success("🚀 **營收創高 (TOP 30)**")
                    display_stock_list_with_data(day_data, "💵 成交金額 (前6名)", "TOP30", "成交金額", stock_info_dict, 6, "#2e7d32", detailed_industry_map)

                st.divider()
                
                # === 策略分組+詳細產業歸納 ===
                st.subheader(f"📅 本月 ({selected_date_str[:7]}) 各策略累積上榜統計 (依產業分類)")
                
                # 在這裡，我們可能需要補抓更多產業資料 (因為月度統計包含非今天的股票)
                # 為了效能，暫時只使用「今天已抓到的」+「預設的」
                # 如果希望更完美，可以在 calculate 裡面再觸發一次 fetch，但會比較慢
                
                strat_tree = calculate_monthly_strategy_grouped(df, selected_date_str, stock_map_global, detailed_industry_map)
                
                if strat_tree:
                    for strat_name, ind_group in strat_tree.items():
                        st.markdown(f"### 📌 {strat_name}")
                        ind_cols = st.columns(3)
                        sorted_inds = sorted(ind_group.items(), key=lambda x: sum(x[1].values()), reverse=True)
                        
                        for i, (industry, counter) in enumerate(sorted_inds):
                            with ind_cols[i % 3]:
                                total_count = sum(counter.values())
                                # 產業名稱特別標示
                                st.markdown(f"**🏭 {industry}** <span style='color:gray; font-size:0.9em'>(累計 {total_count} 次)</span>", unsafe_allow_html=True)
                                for stock, count in counter.most_common():
                                    st.markdown(f"- {stock}: **{count}** 次")
                                st.write("")
                        st.divider()
                else:
                    st.info("本月尚無累計數據。")

                # --- 新聞區塊 ---
                dt_obj = pd.to_datetime(selected_date_str)
                date_range_info = f"{(dt_obj - timedelta(days=30)).strftime('%Y/%m/%d')} ~ {selected_date_str}"
                
                st.subheader(f"📰 相關新聞快訊 (搜尋範圍: {date_range_info})")
                
                if related_news:
                    with st.container():
                        for item in related_news:
                            pub_time = item.published if hasattr(item, 'published') else "未知時間"
                            news_html = f"""
                            <div style="margin-bottom: 2px;">
                                <a href="{item.link}" target="_blank" style="text-decoration: none; color: inherit; font-weight: bold; font-size: 1em;">🔗 {item.title}</a>
                                <br>
                                <span style="font-size: 0.8em; color: gray;">🗓️ {pub_time} | 來源：鉅亨網</span>
                            </div>
                            <hr style="margin: 5px 0; border: 0; border-top: 1px solid #eee;">
                            """
                            st.markdown(news_html, unsafe_allow_html=True)
                else:
                    if not stock_info_dict and not related_news:
                         st.error("⚠️ 偵測到網路連線問題，無法載入外部新聞與報價。")
                    else:
                        st.info(f"在 {date_range_info} 期間，鉅亨網沒有關於這些股票的特定新聞。")

            else:
                st.warning("無資料")
        except Exception as e:
            st.error(f"錯誤: {e}")

    with tab2:
        st.subheader("📊 歷史策略數據")
        all_cols = df.columns.tolist()
        numeric_candidates = [c for c in all_cols if "_TOP" not in c and "日期" not in c and "風度" not in c]
        valid_defaults = [c for c in numeric_candidates if any(k in c for k in ['強勢周', '打工型', '上班強勢'])]
        selected_columns = st.multiselect("👇 選擇指標:", options=numeric_candidates, default=valid_defaults[:4])

        if selected_columns:
            plot_df = df.copy()
            for col in selected_columns: plot_df[col] = pd.to_numeric(plot_df[col], errors='coerce')
            valid_plot_cols = [c for c in selected_columns if not plot_df[c].isna().all()]
            
            if valid_plot_cols:
                preview_cols = ['日期'] + valid_plot_cols
                if '風度' in df.columns: preview_cols.insert(1, '風度')
                
                styler = plot_df[preview_cols].tail(5).style.format({'日期': lambda t: t.strftime('%Y-%m-%d') if pd.notnull(t) else ''})
                for col in valid_plot_cols:
                    styler = styler.format({col: "{:.0f}"})
                    color = '#ffeeba'
                    if '打工' in col: color = '#ffcccc'
                    elif '上班' in col: color = '#cce5ff'
                    elif '趨勢' in col or '強勢' in col: color = '#d4edda'
                    styler = styler.map(make_highlighter(color), subset=[col])
                
                st.dataframe(styler, hide_index=True, use_container_width=True)
                st.plotly_chart(px.line(plot_df.tail(60), x='日期', y=valid_plot_cols, markers=True).update_layout(hovermode="x unified"), use_container_width=True)
            else: st.warning("無有效數據")