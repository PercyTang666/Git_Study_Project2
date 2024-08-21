import datetime
import pandas as pd
import datetime
import pymysql
from jqdatasdk import *
auth("13212099076", "Tjn19960616!")

# import sys
# sys.path.append("/home/cadian/getData/getData_func")
# sys.path.append("/home/cadian/pythonTool/")

from pythonTool import *

from jq_update_func_dol import *


def jq_price_update(start_date=None, end_date=None, fields=["daily"], initialization=False):
    '''
    更新jointquant日行情及其他行情相关数据
    若不指定区间, 则从数据库中最新一天日期(包括)开始，更新数据至最新交易日
    fields 可指定想要更新的数据表:
                                daily: 日线行情 
                                st: 停牌
                                fin_and_sec: 融资融券信息
                                fns_total: 融资融券汇总信息
                                money_flow: 个股资金流向
                                stk_hk_hold: 沪深港通持股数据
                                index_daily: 指数日行情
    initialization默认为False。若为True,则会删除原表后重新写入！！【谨慎使用！！！】
    '''

    
    for field in fields:
        # try:
        jq_update_dailyFormat(field=field, dbPath="dfs://jq_price", dbName="jq_price", start_date=start_date, end_date=end_date, initialization=initialization)
        # except:
        #     print("{} 更新失败".format(field))


def jq_fundamental_update(start_date=None, end_date=None, fields=["indicator"], initialization=False):
    '''
    更新jointquant每日最新财报及指标
    若不指定区间, 则从数据库中最新一天日期(包括)开始，更新数据至最新交易日
    fields 可指定想要更新的数据表:
                                valuation: 估值指标数据
                                indicator: 财务指标数据
                                cash_flow: 现金流量表数据
                                income: 利润表数据
                                balance: 资产负债表数据

    initialization默认为False。若为True,则会删除原表后重新写入！！【谨慎使用！！！】
    '''
 
    for field in fields:
        try:
            jq_update_dailyFormat(field=field, dbPath="dfs://jq_fundamental", dbName="jq_fundamental", start_date=start_date, end_date=end_date, initialization=initialization)
        except:
            print("{} 更新失败".format(field))


def jq_season_update(start_stat=None, end_stat=None, fields=["income_season"], initialization=False):
    '''
    更新jointquant单季度基本面数据库指定区间数据
    若不指定区间, 则默认更新数据库最新报告期及下一报告期数据
    fields 可指定想要更新的数据表:
                                cash_flow_season: 现金流量表数据
                                income_season: 利润表数据
    initialization默认为False。若为True,则会删除原表后重新写入！！【谨慎使用！！！】
    '''
    
    for field in fields:
        try:
            jq_update_seasonFormat(field=field, dbPath="dfs://jq_fundamental_season", dbName="jq_fundamental_season", start_stat=start_stat, end_stat=end_stat, initialization=initialization)
        except:
            print("{} 更新失败".format(field))


def jq_report_update(start_date=None, end_date=None, fields=["balance_sheet"], initialization=False):
    '''
    更新jointquant报告期数据
    若不指定区间, 则默认更新数据库最新pub_date至当天+1自然日
    fields 可指定想要更新的数据表:
                                income_statement: 利润表
                                cashflow_statement: 现金流量表
                                balance_sheet: 资产负债表
    initialization默认为False。若为True,则会删除原表后重新写入！！【谨慎使用！！！】
    '''
    for field in fields:
        try:
            jq_update_dailyFormat_report(field=field, dbPath="dfs://jq_fundamental_report", dbName="jq_fundamental_report", start_date=start_date, end_date=end_date, initialization=initialization)
        except:
            print("{} 更新失败".format(field))


def jq_stockinfo_update():
    '''
    运行该函数, 会在jq_other数据库中生成(覆盖)stock_info表
    '''
    df1 = finance.run_query(query(finance.STK_LIST).filter(finance.STK_LIST.exchange=="XSHE"))
    df2 = finance.run_query(query(finance.STK_LIST).filter(finance.STK_LIST.exchange=="XSHG"))
    df = pd.concat([df1, df2])
    df["start_date"] = df["start_date"].apply(lambda x: pd.to_datetime(x))
    df["end_date"] = df["end_date"].apply(lambda x: pd.to_datetime(x))
    df['id'] = df['id'].astype('int32')
    # 写入个股基本信息
    ddb().create_range_table_to_ddb(data=df, dbPath="dfs://jq_other", dbName="jq_other", tableName="stock_info", partitions=list(range(0, 100000,10000)), partitionColumns="id", initialization=True)
    print("********** 股票基本信息写入完成 **********")

    return


def jq_industry_update(start_date=None, end_date=None, initialization=False, industry_type=["sw_l1", "sw_l2", "sw_l3"]):
    '''
    更新jointquant个股所属行业数据
    若不指定区间, 则默认更新数据至最新交易日(若数据库已更新至最新, 则默认将最近交易日数据进行更新)
    industry_type 可指定想要更新的数据表:
                    sw_l1: 申万一级行业
                    sw_l2: 申万二级行业
                    sw_l3: 申万三级行业
                    zjw: 证监会所属行业
                    jq_l2: 聚宽二级行业
                    jq_l1": 聚宽一级行业
    initialization默认为False。若为True,则会删除原表后重新写入！！【谨慎使用！！！】
    '''
    jq_update_dailyFormat(start_date=start_date, end_date=end_date, field="industry", initialization=initialization, industry_type=industry_type)
    return


def jq_tradedays_update(start_date="2005-01-01", end_date=None):
    '''
    运行该函数, 会在jq_data数据库中生成(覆盖)trade_cal表, 包含从start_date到end_date所有上交所交易日
    '''

    if end_date==None:
        end_date_dateFormat = datetime.datetime.today() + datetime.timedelta(days=365)
        end_date = end_date_dateFormat.strftime("%Y-%m-%d")

    df = pd.DataFrame(get_trade_days(start_date,end_date),columns=['date'])
    df["date"] = df["date"].apply(lambda x: pd.to_datetime(x))

    # 写入新交易日历
    ddb().drop_table_from_ddb(dbPath="dfs://jq_price", tableName="trade_cal")
    ddb().create_value_table_to_ddb(data=df, dbPath="dfs://jq_price", dbName="jq_price", tableName="trade_cal", partition_startdate="2005-01-01", partition_enddate="2066-12-31", partition_period="Y", partitionColumns="date")
    print("\n********** 新交易日历写入完成 **********")

    return


def jq_minutebar_update(start_date=None, end_date=None, fields=["minbar"], initialization=False):
    '''
    更新jointquant分钟级k线
    若不指定区间, 则默认从数据库中最新日期后退一个交易日开始，更新至当天
    fields 可指定想要更新的数据表:
                                minute: 1分钟k线, 
    initialization默认为False。若为True,则会删除原表后重新写入！！【谨慎使用！！！】
    '''
 
    for field in fields:
        jq_update_minuteFormat(field=field, dbPath="dfs://jq_minute", dbName="jq_minute", start_date=start_date, end_date=end_date, initialization=initialization)


if __name__ == "__main__":

    start_date = None
    end_date = None

    # 更新jq_price数据库(日频行情相关数据)
    jq_price_update(start_date=start_date, end_date=end_date,
                    fields=["daily", "st", "fin_and_sec", "fns_total", "money_flow", "stk_hk_hold", "index_daily",
                            "index_components"], initialization=False)

    # 更新股票信息
    jq_stockinfo_update()

    # 更新jq数据库申万行业分类数据
    jq_industry_update(start_date=start_date, end_date=end_date, initialization=False)

    # 更新交易日历
    jq_tradedays_update()

    # 更新jq_fundamental数据库(日频基本面相关数据)
    jq_fundamental_update(start_date=start_date, end_date=end_date, fields = ["indicator", "valuation"], initialization=False)


    # # # 更新jq_fundamental_season单季度财务数据(根据报告期更新单季度数据, 默认每次更新系统中最新报告期及下一报告期)
    # jq_season_update(start_stat=start_stat, end_stat=end_stat, fields=["income_season","cashflow_season"], initialization=False)
    #
    # # # 更新jq_fundamental_report报告期财务数据(根据pub_date更新报告期数据, 默认每次更新系统最新pub_date到当前日期+1自然日)
    # jq_report_update(start_date=start_date, end_date=end_date, fields=["income_statement","cashflow_statement","balance_sheet", "fin_forcast"], initialization=False)
    #
    # # # 更新wind数据
    # wind_data_update(start_date=start_date, end_date=end_date, fields=["AShareMoneyFlow","ConsensusRollingData"], initialization=False)

    # # 更新jq分钟线数据
    # jq_minutebar_update(start_date=start_date, end_date=end_date, fields=["minbar5","minbar15","minbar30","minbar60"], initialization=False)
    #
    # # 更新MSCI id对照表数据
    # msci_ids_update(start_date=start_date, end_date=end_date, msci_con = msci_con)

    
    logout()


    ########################################### 废弃(jointquant无法读取单季度全部报告期数据，故不适用以下接口进行更新, 否则会造成数据更新不全) ################################################

    # #更新jq_fundamental_report报告期财务数据(根据报告期更新报告期数据, 默认每次更新系统中最新报告期及下一报告期), 不建议使用该脚本进行日常维护
    # jq_report_update_v2(start_stat=start_stat, end_stat=end_stat, fields = ["income_statement","cashflow_statement","balance_sheet"], initialization=False)

    ########## 2023.06.29开始 1分钟k线里面涨跌停的价格是错误的 后续从06.29开始覆盖重新更新