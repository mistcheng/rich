# 导入futu-api
import sys
import getopt
import futu as ft
import pymysql.cursors


def add_kline(kline_list):
    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 db='futu',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            for kline in kline_list:
                # Create a new record
                sql = "INSERT INTO `kline` (`code`, `time_key`, `open`, `close`, `high`, `low`, `pe_ratio`, " \
                      "`turnover_rate`, `volume`, `turnover`, `change_rate`, `last_close`) VALUES (%s, %s, %f, " \
                      "%f, %f, %f, %f, %f, %d, %f, %f, %f)"
                cursor.execute(sql, (kline['code'], kline['time_key'], kline['open'], kline['close'], kline['high'],
                                     kline['low'], kline['pe_ratio'], kline['turnover_rate'], kline['volume'],
                                     kline['turnover'], kline['change_rate'], kline['last_close']))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        connection.close()


def get_history_kline(code_list, start=None, end=None, ktype=ft.KLType.K_DAY):
    # 实例化行情上下文对象
    quote_ctx = ft.OpenQuoteContext(host="127.0.0.1", port=11111)

    # 上下文控制
    quote_ctx.start()  # 开启异步数据接收
    quote_ctx.set_handler(ft.TickerHandlerBase())  # 设置用于异步处理数据的回调对象(可派生支持自定义)

    # 低频数据接口
    market = ft.Market.HK

    # 高频数据接口
    quote_ctx.subscribe(code_list, [ft.SubType.QUOTE, ft.SubType.TICKER, ft.SubType.K_DAY, ft.SubType.ORDER_BOOK,
                               ft.SubType.RT_DATA, ft.SubType.BROKER])
    # 查询历史K线，保存进数据库
    for code in code_list:
        ret, data, page_req_key = quote_ctx.request_history_kline(code, start=start, end=end, ktype=ktype)
        if ret != ft.RET_OK:
            print(data)  # print msg
            continue
        add_kline(data.DataFrame)

    # 停止异步数据接收
    quote_ctx.stop()

    # 关闭对象
    quote_ctx.close()


def main(argv):
    code = 'HK.00700'
    code_list = [code]
    get_history_kline(code_list)

    # username = ""
    # password = ""
    #
    # try:
    #     opts, args = getopt.getopt(argv, "hu:p:", ["help", "username=", "password="])
    # except getopt.GetoptError:
    #     print('Error: test_arg.py -u <username> -p <password>')
    #     print('   or: test_arg.py --username=<username> --password=<password>')
    #     sys.exit(2)
    #
    # # 处理 返回值options是以元组为元素的列表。
    # for opt, arg in opts:
    #     if opt in ("-h", "--help"):
    #         print('test_arg.py -u <username> -p <password>')
    #         print('or: test_arg.py --username=<username> --password=<password>')
    #         sys.exit()
    #     elif opt in ("-u", "--username"):
    #         username = arg
    #     elif opt in ("-p", "--password"):
    #         password = arg
    # print('username为：', username)
    # print('password为：', password)


if __name__ == "__main__":
    main(sys.argv[1:])

