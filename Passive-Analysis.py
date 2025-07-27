"""
整合后的股票分析工具
功能：
1. 从Baostock获取低PE股票并保存。
2. 下载低PE股票的K线数据并保存。
3. 对K线数据进行低波动股票筛选（原BS_Kline_range.py的选项4）。
4. 生成筛选后的股票列表和K线图。
"""

# ======================= 导入依赖 =======================
import baostock as bs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import re
import sys
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor

# ======================= 可视化模块 (来自 BS_Kline_range.py) =======================
def plot_kline_plotly(ohlc_data, code, stock_name, save_dir, latest_amount, latest_date, latest_pe):
    """使用Plotly生成专业K线图表"""
    trade_dates = ohlc_data.index.strftime('%Y-%m-%d').tolist()
    x_sequence = list(range(len(trade_dates)))
    
    fig = go.Figure(data=[go.Candlestick(
        x=x_sequence,
        open=ohlc_data['Open'],
        high=ohlc_data['High'],
        low=ohlc_data['Low'],
        close=ohlc_data['Close'],
        increasing_line_color='red',
        decreasing_line_color='green',
        name='K线',
        hovertext=trade_dates,
        hoverinfo="x+y+text"
    )])
    
    fig.add_trace(go.Bar(
        x=x_sequence,
        y=ohlc_data['Volume'],
        name='成交额(亿元)',
        marker_color='rgba(100, 150, 200, 0.6)',
        yaxis='y2',
        hoverinfo="x+y"
    ))
    
    step = max(1, len(x_sequence)//10)
    visible_dates = [d if i%2 == 0 else '' for i, d in enumerate(trade_dates[::step])]
    
    fig.update_xaxes(
        tickvals=x_sequence[::step],
        ticktext=visible_dates,
        title_text='交易日序列',
        showgrid=True,
        rangeslider=dict(visible=False)
    )
    
    fig.update_layout(
        title=dict(
            text=f'{stock_name}({code}) {latest_date.strftime("%Y-%m-%d")} 市盈率:{latest_pe:.1f}',
            font=dict(size=24),
            x=0.05,
            y=0.95
        ),
        yaxis_title='价格',
        yaxis2=dict(
            title='成交额(亿元)',
            overlaying='y',
            side='right'
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        width=1200,
        height=800,
        margin=dict(l=50, r=50, b=50, t=100),
        font=dict(size=14)
    )
    # 清理股票名称中的非法文件名字符
    clean_stock_name = re.sub(r'[\\/:*?"<>|]', '_', stock_name)
    save_path = os.path.join(save_dir, f"{code}_{clean_stock_name}_K线图.png")
    fig.write_image(save_path, scale=2)

# ======================= 数据分析模块 (来自 BS_Kline_range.py) =======================
def is_continuous_decline(close_prices, window=5):
    """判断股票是否连续下跌（默认5日）"""
    valid_closes = []
    for date_str, price in close_prices.items():
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            valid_closes.append((date, float(price)))
        except:
            continue
    
    valid_closes.sort(key=lambda x: x[0])
    
    if len(valid_closes) < window:
        return False
    
    last_window = valid_closes[-window:]
    return all(last_window[i][1] > last_window[i+1][1] for i in range(window-1))

def calculate_price_change(close_prices, window=10):
    """计算指定窗口期的价格变化百分比"""
    valid_closes = []
    for date_str, price in close_prices.items():
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            valid_closes.append((date, float(price)))
        except:
            continue
    
    if not valid_closes:
        return None
    
    valid_closes.sort(reverse=True, key=lambda x: x[0])
    
    if len(valid_closes) < window:
        return None
    
    selected_dates = []
    current_date = valid_closes[0][0]
    for date, price in valid_closes:
        if len(selected_dates) >= window:
            break
        if date <= current_date:
            selected_dates.append((date, price))
            current_date = date - pd.tseries.offsets.BDay(1)
    
    if len(selected_dates) < window:
        return None
    
    start_price = selected_dates[-1][1]
    end_price = selected_dates[0][1]
    return (end_price - start_price) / start_price * 100

def check_low_volatility(pct_changes, window_days, max_daily_gain, max_daily_loss, min_cumulative):
    """
    检查低波动性及累计涨幅
    条件：
    1. 每日涨跌幅在[max_daily_loss, max_daily_gain]范围内
    2. 累计涨幅≥min_cumulative%
    """
    valid_changes = []
    for date_str, change in pct_changes.items():
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            valid_changes.append((date, float(change)))
        except:
            continue
    
    if len(valid_changes) < window_days:
        return False
    
    valid_changes.sort(reverse=True, key=lambda x: x[0])
    recent_changes = valid_changes[:window_days]
    
    total_gain = 0.0
    for _, change in recent_changes:
        if change > max_daily_gain or change < max_daily_loss:
            return False
        total_gain += change
    
    return total_gain >= min_cumulative

# ======================= 整合后的K线生成与筛选逻辑 =======================

def analyze_and_generate_kline_charts(filename, custom_params=None):
    """
    整合后的分析和K线图生成函数，直接执行低波动股票筛选逻辑。
    :param filename: K线数据文件路径 (stock_kline_data.xlsx)
    :param custom_params: 筛选参数字典
    """
    print("" + "="*50)
    print("  步骤3: 低波动股票筛选与K线图生成")
    print("="*50 + "")

    # 默认参数 (来自 BS_Kline_range.py 的选项4默认值)
    params = {
        'pe_min': 0,
        'pe_max': 30,
        'amount_min': 2, # 默认2亿元，原GUI中是0.5，这里为了更严格筛选，使用2
        'window_days': 10,
        'min_cumulative': 4.0,
        'max_daily_gain': 5.0,
        'max_daily_loss': -5.0
    }
    if custom_params:
        params.update(custom_params)

    try:
        df = pd.read_excel(filename, index_col="股票代码")
        
        date_columns = [col.split('_')[1] for col in df.columns if '收盘价_' in col]
        
        current_date = datetime.now()
        future_dates = []
        for d in date_columns:
            try:
                date_obj = datetime.strptime(d, "%Y-%m-%d")
                if date_obj > current_date:
                    future_dates.append(d)
            except:
                continue
        if future_dates:
            print(f"警告：数据包含未来日期 {future_dates}")
        
        sorted_dates = sorted([datetime.strptime(d, "%Y-%m-%d") for d in date_columns])
        date_range = f"{sorted_dates[0].strftime('%Y-%m-%d')} 至 {sorted_dates[-1].strftime('%Y-%m-%d')}"
        latest_date_str = sorted_dates[-1].strftime("%Y-%m-%d")
        print(f"数据日期范围: {date_range} | 最新交易日: {latest_date_str}")
    except Exception as e:
        print(f"× 文件读取失败: {str(e)}")
        return

    rs = bs.query_stock_basic()
    code_to_name = {}
    while (rs.error_code == '0') and rs.next():
        row_data = rs.get_row_data()
        code_to_name[row_data[0]] = row_data[1]

    pe_columns = [col for col in df.columns if col.startswith('市盈率_')]
    if not pe_columns:
        print(" 文件中缺少市盈率数据")
        return
    
    latest_pe_col = max(
        pe_columns,
        key=lambda x: datetime.strptime(x.split('_')[1], "%Y-%m-%d")
    )

    valid_stocks = []
    for code in df.index:
        try:
            latest_pe = df.loc[code, latest_pe_col]
            
            if pd.isna(latest_pe) or not (params['pe_min'] <= latest_pe <= params['pe_max']):
                continue
            
            close_cols = [col for col in df.columns if '收盘价_' in col]
            close_prices = {col.split('_')[1]: df.loc[code, col] for col in close_cols}
            
            if is_continuous_decline(close_prices):
                print(f"排除 {code}（连续5日下跌）")
                continue
                
            price_change = calculate_price_change(close_prices)
            if price_change is not None:
                if price_change <= -5:
                    print(f"排除 {code}（近10日累计下跌{abs(price_change):.1f}%）")
                    continue
            else:
                print(f"警告：{code} 数据不足，跳过跌幅检查")
            
            # 低波动筛选条件
            pct_cols = [col for col in df.columns if '涨幅_' in col]
            pct_changes = {col.split('_')[1]: df.loc[code, col] for col in pct_cols}
            
            if not check_low_volatility(
                pct_changes,
                params['window_days'],
                params['max_daily_gain'],
                params['max_daily_loss'],
                params['min_cumulative']
            ):
                print(f"排除 {code}（近{params['window_days']}日波动率或累计涨幅不达标）")
                continue
            
            latest_amount_col = f"成交额(亿元)_{latest_date_str}"
            if latest_amount_col not in df.columns:
                print(f"排除 {code}（缺少成交额数据）")
                continue
            
            latest_amount = df.loc[code, latest_amount_col]
            if pd.isna(latest_amount) or latest_amount < params['amount_min']:
                print(f"排除 {code}（最后交易日成交额{latest_amount:.3f}亿元 < {params['amount_min']}亿元）")
                continue
                
            valid_stocks.append(code)
        except Exception as e:
            print(f"处理 {code} 时出错: {str(e)}")
            continue

    if not valid_stocks:
        print("没有符合筛选条件的股票")
        return

    output_filename = "low_volatility_stocks.xlsx"
    
    # 提取股票代码中的数字部分，并保持前导零
    numeric_codes = [re.search(r'\d+', code).group(0) for code in valid_stocks]
    result_df = pd.DataFrame({"股票代码": numeric_codes})

    # 使用 'xlsxwriter' 引擎并设置文本格式来保存
    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook  = writer.book
        worksheet = writer.sheets['Sheet1']
        text_format = workbook.add_format({'num_format': '@'})
        worksheet.set_column('A:A', 12, text_format) # 设置A列宽度为12

    print(f"已保存筛选结果到: {output_filename}")

    print("" + "="*50)
    print("低波动股票按行业分类列表：")
    
    industry_dict = {}
    for code in valid_stocks:
        try:
            rs_industry = bs.query_stock_industry(code=code)
            while (rs_industry.error_code == '0') and rs_industry.next():
                industry_data = rs_industry.get_row_data()
                industry_type = industry_data[3]
                
                industry_type = industry_type.strip() if industry_type else "未知行业"
                
                if industry_type not in industry_dict:
                    industry_dict[industry_type] = []
                industry_dict[industry_type].append( (code, code_to_name.get(code, "未知股票")))
        except Exception as e:
            print(f" 获取 {code} 行业信息失败: {str(e)}")
            if "未知行业" not in industry_dict:
                industry_dict["未知行业"] = []
            industry_dict["未知行业"].append( (code, code_to_name.get(code, "未知股票")))

    sorted_industries = sorted(industry_dict.items(), key=lambda x: x[0])
    
    for industry, stocks in sorted_industries:
        print(f"\n■ 行业分类：{industry}（共{len(stocks)}只）")
        print() # 添加空行，使行业名称单独成段
        for idx, (code, name) in enumerate(stocks, 1):
            print(f"{idx:2d}. 代码：{code.ljust(10)} | 名称：{name}", end='\n' if idx%2==0 else '\t')
        if len(stocks) % 2 != 0: # 如果股票数量为奇数，确保最后一行有换行
            print()
    print("\n" + "="*50)

    output_dir = "K_line_charts"
    os.makedirs(output_dir, exist_ok=True)

    batch_size = 2000
    total_batches = (len(valid_stocks) + batch_size - 1) // batch_size
    current_batch = 0

    ohlc_cols = {
        'Open':  [col for col in df.columns if '开盘价_' in col],
        'High':  [col for col in df.columns if '最高价_' in col],
        'Low':   [col for col in df.columns if '最低价_' in col],
        'Close': [col for col in df.columns if '收盘价_' in col],
        'Volume': [col for col in df.columns if '成交额(亿元)_' in col]
    }

    def process_code(code):
        """单个股票处理函数（供线程池调用）"""
        try:
            stock_name = code_to_name.get(code, "未知股票")
            
            dates = [col.split('_')[1] for col in ohlc_cols['Open']]
            ohlc_data = pd.DataFrame({
                'Open':  df.loc[code, ohlc_cols['Open']].values,
                'High':  df.loc[code, ohlc_cols['High']].values,
                'Low':   df.loc[code, ohlc_cols['Low']].values,
                'Close': df.loc[code, ohlc_cols['Close']].values,
                'Volume': df.loc[code, ohlc_cols['Volume']].values
            }, index=pd.to_datetime(dates))

            ohlc_data = ohlc_data.sort_index().astype(float)
            ohlc_data.replace([np.inf, -np.inf], np.nan, inplace=True)
            ohlc_data.dropna(inplace=True)

            if ohlc_data.empty:
                print(f" {code} 数据无效，跳过")
                return

            latest_date = ohlc_data.index[-1]
            latest_amount = ohlc_data.iloc[-1]['Volume']
            latest_pe = df.loc[code, latest_pe_col]
            
            plot_kline_plotly(
                ohlc_data=ohlc_data,
                code=code,
                stock_name=stock_name,
                save_dir=output_dir,
                latest_amount=latest_amount,
                latest_date=latest_date,
                latest_pe=latest_pe
            )
        except Exception as e:
            print(f"{code} 生成失败: {str(e)}")

    while current_batch < total_batches:
        start_idx = current_batch * batch_size
        end_idx = min((current_batch + 1) * batch_size, len(valid_stocks))
        batch = valid_stocks[start_idx:end_idx]

        print("" + "="*50)
        print(f"开始生成第 {current_batch + 1}/{total_batches} 批（共{len(batch)}只股票）")
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(process_code, batch))

        current_batch += 1

# ======================= 主程序入口 =======================

if __name__ == "__main__":
    lg = None
    try:
        # 依赖检查
        try:
            import baostock
            import plotly
            import pandas
        except ImportError as e:
            print(f"错误：缺少必要依赖库 - {str(e)}")
            print("请执行以下命令安装依赖：")
            print("pip install baostock pandas numpy plotly openpyxl")
            sys.exit(1)

        # 登录BaoStock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"Baostock 登录失败: {lg.error_msg}")
            sys.exit(1)

        # 步骤1：低波动股票筛选与K线图生成
        # 文件名固定为 stock_kline_data.xlsx，因为数据将由 GitHub Actions 下载到当前目录
        kline_filename = "stock_kline_data.xlsx"
        analyze_and_generate_kline_charts(kline_filename, custom_params=None)

    except Exception as e:
        print(f"任务执行过程中发生未预期错误: {e}")

    finally:
        if lg:
            bs.logout()
            print(" Baostock 已登出。")

    print(f"{'='*60}本次任务执行完毕。{'='*60}")