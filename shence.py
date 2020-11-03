#!/usr/bin/env python3
# -*- coding: utf-8 -*-:

"""
File:shence.py 
Anthor: cuikaibin
Date: 2020/4/15
"""

import os
import xlrd
import argparse
import threading
import requests


def shence_api(token, project, sql_command):
    headers = {'Content-type': 'application/x-www-form-urlencode'}
    url = '***'
    data = {\
        'token': token,
        'project': project,
        'format': 'csv',
        'q': sql_command}
    try:
        s = requests.post(url, data=data)
        return s.text #s.text 的类型为str
    except Exception as e:
        raise e
        return 'request fail'


def xls_read(file_name):
    room_dir = os.path.abspath('.')
    xls_path = room_dir + '/' + file_name
    workbook = xlrd.open_workbook(xls_path)
    name_list = workbook.sheet_names()

    events_index = name_list.index('events')
    events_sheet = workbook.sheets()[events_index]
    events_data_list = []
    events_base_data_list = []   
    events_erowNum = events_sheet.nrows  # sheet行数
    events_colNum = events_sheet.ncols  # sheet列数
    events_merge = events_sheet.merged_cells #获取合并单元格的坐标
    for r in range(1, events_erowNum):
        li = []
        for c in range(events_colNum):
            # 读取每个单元格里的数据，合并单元格只有单元格内的第一行第一列有数据，其余空间都为空
            cell_value = events_sheet.row_values(r)[c]
            # 判断空数据是否在合并单元格的坐标中，如果在就把数据填充进去
            if cell_value is None or cell_value == '':
                for (rlow, rhigh, clow, chigh) in events_merge:
                    if rlow <= r < rhigh:
                        if clow <= c < chigh:
                            cell_value = events_sheet.cell_value(rlow, clow)
            li.append(cell_value)
        events_data_list.append(li)

    for r in range(1, events_erowNum):
        li = []
        for c in range(2):
            # 读取每个单元格里的数据
            cell_value = events_sheet.cell_value(r, c)
            li.append(cell_value)
        if li[0]:
            events_base_data_list.append(li)  

    if 'base' in name_list:
        base_index = name_list.index('base')
        base_sheet = workbook.sheets()[base_index]
        base_data_list = []
        base_rowNum = base_sheet.nrows  # sheet行数
        base_colNum = base_sheet.ncols  # sheet列数
        for r in range(1, base_rowNum):
            li = []
            for c in range(base_colNum):
                # 读取每个单元格里的数据
                cell_value = base_sheet.cell_value(r, c)
                li.append(cell_value)
            base_data_list.append(li)

        for events_base_data in events_base_data_list:
            for base_data in base_data_list:
                events_data_list.append(events_base_data + base_data)

    if 'users' in name_list:
        users_index = name_list.index('users')
        users_sheet = workbook.sheets()[users_index]
        users_rowNum = users_sheet.nrows  # sheet行数
        users_colNum = users_sheet.ncols  # sheet列数
        users_data_list =[]
        users_joint_data_list = []
        for r in range(1, users_rowNum):
            li = []
            for c in range(users_colNum):
                # 读取每个单元格里的数据
                cell_value = users_sheet.cell_value(r, c)
                li.append(cell_value)
            users_data_list.append(li)

        for events_base_data in events_base_data_list:
            for users_data in users_data_list:
                users_joint_data_list.append(events_base_data + users_data)
    else:
        users_joint_data_list =[]

    excel_data = {
        'events': events_data_list,
        'users': users_joint_data_list}

    return excel_data


def data_handle(token, project, sql_command, sql_parameter_list, table_name):
    """
    @param model int 1:没有给取值示例;  2:给到了取值示例
    """
    response_result = shence_api(token=token, project=project, sql_command=sql_command)
    response_result_list = response_result.split('\n')

    response_result_list_len = len(response_result_list) 
    #累计查询错误的值
    total = 0

    for i in range(response_result_list_len-2):
        if table_name == 'events':
            if sql_parameter_list[4]  == '' and sql_parameter_list[2] == 'stu_id':
                value_list = response_result_list[i+1].split( )
                if value_list[0] == value_list[1]:
                    pass
                else:
                    total += 1
            elif sql_parameter_list[4]  == '':
                if response_result_list[0] != sql_parameter_list[2]:
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] == '':
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] != '':
                    pass
            elif sql_parameter_list[3] == '字符串' and sql_parameter_list[2] == 'purchase_status':
                value_list = response_result_list[i+1].split( )
                if value_list[0] == '-1' and len(value_list) == 1:
                    pass
                elif value_list[0] != '-1' and value_list[1] != '':
                    pass
                else:
                    total += 1
            elif sql_parameter_list[3] == '字符串' or sql_parameter_list[3] == '数值' or sql_parameter_list[3] == '数字':
                if response_result_list[0] != sql_parameter_list[2]:
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] == '':
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] != '':
                    pass 
                # TODO: 查询结果类型的校验
                # if sql_key_list[3] == '字符串' and type(response_result_list[i+1]) == str:
                #     print ('第{}条数据查询成功'.format(i+1))
                #     pass
                # elif sql_key_list[3] == '数值' and type(response_result_list[i+1]) == int:
                #     print ('第{}条数据查询成功'.format(i+1))
                #     pass
                # elif sql_key_list[3] == '日期' and type(response_result_list[i+1]) == 时间:
                #     print ('第{}条数据查询成功'.format(i+1))
                #     pass
                # else:
                #     total += 1
        elif table_name == 'users':              
            if sql_parameter_list[4]  == '':
                if response_result_list[0] != sql_parameter_list[2]:
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] == '':
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] != '':
                    pass
            elif sql_parameter_list[3] == '字符串' or ql_parameter_list[3] == '数值' or sql_parameter_list[3] == '数字':
                if response_result_list[0] != sql_parameter_list[2]:
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] == '':
                    total += 1
                elif response_result_list[0] == sql_parameter_list[2] and response_result_list[i+1] != '':
                    pass

    if total == 0 and response_result_list_len-2 != 0:
        # print ('-------------------------------------------------------')
        # print (response_result_list)
        # print ('{}表{}事件{}参数查询了{}条数据,数据全部正确'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2))
        # print ('sql命令为{}'.format(sql_command))
        pass
    elif total == response_result_list_len-2:
        if sql_parameter_list[2] == 'stu_id':
            print ('---------------------------------------------------------------------------------------')
            print (response_result_list)
            print ('stu_id,distinct_id成对校验, 当stu_id不为空时，stu_id==distinct_id')
            print ('{}表{}事件{}参数查询了{}条数据,数据全部为空or错误'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2))
            print ('sql命令为{}'.format(sql_command))            
        elif sql_parameter_list[2] == 'purchase_status':
            print ('---------------------------------------------------------------------------------------')
            print (response_result_list)
            print ("purchase_status,stu_id成对校验, purchase_status=='-1'时，stu_id才可为空")
            print ('{}表{}事件{}参数查询了{}条数据,数据全部为空or错误'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2))
            print ('sql命令为{}'.format(sql_command))
        else:
            print ('---------------------------------------------------------------------------------------')
            print (response_result_list)
            print ('{}表{}事件{}参数查询了{}条数据,数据全部为空or错误'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2))
            print ('sql命令为{}'.format(sql_command))
    else:
        if sql_parameter_list[2] == 'stu_id':
            print ('---------------------------------------------------------------------------------------')
            print (response_result_list)
            print ('stu_id,distinct_id成对校验, 当stu_id不为空时，stu_id==distinct_id')
            print ('{}表{}事件{}参数查询了{}条数据,其中{}条数据为空or错误'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2, total))
            print ('sql命令为{}'.format(sql_command))
        elif sql_parameter_list[2] == 'purchase_status':
            print ('---------------------------------------------------------------------------------------')
            print (response_result_list)
            print ("purchase_status,stu_id成对校验, purchase_status=='-1'时，stu_id才可为空")
            print ('{}表{}事件{}参数查询了{}条数据,其中{}条数据为空or错误'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2, total))
            print ('sql命令为{}'.format(sql_command))
        else:
            print ('---------------------------------------------------------------------------------------')
            print (response_result_list)
            print ('{}表{}事件{}参数查询了{}条数据,其中{}条数据为空or错误'.format(table_name, sql_parameter_list[0], sql_parameter_list[2], response_result_list_len-2, total))
            print ('sql命令为{}'.format(sql_command))


def sql_command_method(sql_parameter_list, table_name, system, version, number, sql_value=''):
    """
    @param sql_value           srt|int   参数的可能值
    @param sql_parameter_list  list      示例['training_game_home', '能力训练场_游戏_首页', 'is_fullstar', '数值', '1、0']
    """
    if table_name == 'events':
        #stu_id特殊校验，当stu_id不为空时，stu_id==distinct_id
        if sql_parameter_list[4]  == '' and sql_parameter_list[2] == 'stu_id':
            sql_command = "select stu_id,distinct_id from events where event=\'{}\' and $lib like \'{}\' and hmk_build_version like \'{}\' and event_displayname=\'{}\' and stu_id<>'' order by time desc limit {}".\
                format(sql_parameter_list[0], system, version, sql_parameter_list[1], number)
        elif sql_parameter_list[4]  == '':
            sql_command = "select {} from events where event=\'{}\' and $lib like \'{}\' and hmk_build_version like \'{}\' and event_displayname=\'{}\' order by time desc limit {}".\
                format(sql_parameter_list[2], sql_parameter_list[0], system, version, sql_parameter_list[1], number)
        #purchase_status做特殊处理，purchase_status=='-1'时，stu_id为空
        elif sql_parameter_list[3] == '字符串' and sql_parameter_list[2] == 'purchase_status':
            sql_command = "select purchase_status,stu_id from events where event=\'{}\' and $lib like \'{}\' and {}=\'{}\' and hmk_build_version like \'{}\' and event_displayname=\'{}\' order by time desc limit {}".\
                format(sql_parameter_list[0], system, sql_parameter_list[2], sql_value, version, sql_parameter_list[1], number)
        elif sql_parameter_list[3] == '字符串':
            sql_command = "select {} from events where event=\'{}\' and $lib like \'{}\' and {}=\'{}\' and hmk_build_version like \'{}\' and event_displayname=\'{}\' order by time desc limit {}".\
                format(sql_parameter_list[2], sql_parameter_list[0], system, sql_parameter_list[2], sql_value, version, sql_parameter_list[1], number)
        elif sql_parameter_list[3] == '数值' or sql_parameter_list[3] == '数字':
            sql_command = "select {} from events where event=\'{}\' and $lib like \'{}\' and {}={} and hmk_build_version like \'{}\' and event_displayname=\'{}\' order by time desc limit {}".\
                format(sql_parameter_list[2], sql_parameter_list[0], system, sql_parameter_list[2], sql_value, version, sql_parameter_list[1], number)
    elif table_name == 'users':
        if sql_parameter_list[4]  == '':
            sql_command = "select users.{} from users inner join events on users.stu_id=events.stu_id where events.event=\'{}\' and events.$lib like \'{}\' and events.distinct_id<>'-1' by time desc limit {}".\
                format(sql_parameter_list[2], sql_parameter_list[0], system, number)
        elif sql_parameter_list[3] == '字符串':
            sql_command = "select users.{} from users inner join events on users.stu_id=events.stu_id where events.event=\'{}\' and events.$lib like \'{}\' and users.{}=\'{}\' and events.distinct_id<>'-1' order by time desc limit {}".\
                format(sql_parameter_list[2], sql_parameter_list[0], system, sql_parameter_list[2], sql_value, number)
        elif sql_parameter_list[3] == '数值' or sql_parameter_list[3] == '数字':
            sql_command = "select users.{} from users inner join events on users.stu_id=events.stu_id where events.event=\'{}\' and events.$lib like \'{}\' and users.{}={} and events.distinct_id<>'-1' order by time desc limit {}".\
                format(sql_parameter_list[2], sql_parameter_list[0], system, sql_parameter_list[2], sql_value, number)
    return sql_command


def thread_run(file_name, system, token, project, version, number):
    """
    @param file_name str 埋点校验文件名
    @param system    str 操作系统,该参数服务端不校验
    @param token     str 神策接口参数
    @param project   str 神策接口参数
    @param version   str ios、android版本号，选填
    @param number    str 单个埋点校验条数
    """
    request_list = []
    excel_data = xls_read(file_name=file_name)
    events_sql_parameter_list = excel_data['events']
    users_sql_parameter_list = excel_data['users']

    #sql_parameter_list示例['training_game_home', '能力训练场_游戏_首页', 'is_fullstar', '数值', '1、0']
    for sql_parameter_list in events_sql_parameter_list:
        if sql_parameter_list[4] == '' and sql_parameter_list[2] != '':
            sql_command = sql_command_method(sql_parameter_list=sql_parameter_list, table_name='events', system=system, version=version, number=number)
            request_thread = threading.Thread(target=data_handle, args=(token, project, sql_command, sql_parameter_list, 'events'))
            request_list.append(request_thread)
        elif sql_parameter_list[2] != '':
            if type(sql_parameter_list[4]) == float:
                sql_parameter_list[4] = int(sql_parameter_list[4])
            sql_parameter_list[4] = str(sql_parameter_list[4])
            sql_values = sql_parameter_list[4].split('；')
            for sql_value in sql_values:
                sql_command = sql_command_method(sql_parameter_list=sql_parameter_list, table_name='events', system=system, version=version, number=number, sql_value=sql_value)
                request_thread = threading.Thread(target=data_handle, args=(token, project, sql_command, sql_parameter_list, 'events'))
                request_list.append(request_thread)

    #['picbook_bookdetail_readingresult_click', '绘本_绘本详情_读绘本结果_点击', 'gender', '字符串', '男、女']
    if len(users_sql_parameter_list) != 0:
        for sql_parameter_list in users_sql_parameter_list:
            if sql_parameter_list[4] == '' and sql_parameter_list[2] != '':
                sql_command = sql_command_method(sql_parameter_list=sql_parameter_list, table_name='users', system=system, version=version, number=number)
                request_thread = threading.Thread(target=data_handle, args=(token, project, sql_command, sql_parameter_list, 'users'))
                request_list.append(request_thread)
            elif sql_parameter_list[2] != '':
                if type(sql_parameter_list[4]) == float:
                    sql_parameter_list[4] = int(sql_parameter_list[4])
                sql_parameter_list[4] = str(sql_parameter_list[4])
                sql_values = sql_parameter_list[4].split('；')
                for sql_value in sql_values:
                    sql_command = sql_command_method(sql_parameter_list=sql_parameter_list, table_name='users', system=system, version=version, number=number, sql_value=sql_value)
                    request_thread = threading.Thread(target=data_handle, args=(token, project, sql_command, sql_parameter_list, 'users'))
                    request_list.append(request_thread)

    for request_thread in request_list:
        request_thread.start()
        request_thread.join()


if __name__ == '__main__':
    MONKEY_TOKEN_TEST = '**'
    SYSTEM_IOS = 'iOS'
    SYSTEM_ANDROID = 'Android'
    SYSTEM_H5 = 'js'
    SYSTEM_MINI = 'MiniProgram'
    #ENGLIST_PROJECT = 'monkeyabc'
    #CHINESE_PROJECT = 'imonkey'
    TEST_PROJECT = 'default'

    parser = argparse.ArgumentParser(description='manual to this script')
    #parser.add_argument('--token', type=str, default='monkey_test')
    parser.add_argument('--file_name', type=str, default='h.xlsx')
    parser.add_argument('--system', type=str, default='h5')
    parser.add_argument('--version', type=str, default='%')
    parser.add_argument('--number', type=str, default='5')
    args = parser.parse_args()

    """
    js     校验H5的埋点
    mini   校验小程序的埋点
    """
    if args.system.lower() == 'ios':
        system = SYSTEM_IOS
    elif args.system.lower() == 'android':
        system = SYSTEM_ANDROID
    elif args.system.lower() == 'h5':
        system = SYSTEM_H5
    elif args.system.lower() == 'MiniProgram':
        system = SYSTEM_MINI
    elif args.system.lower() == 'server':
        system = '%'
    else:
        print ('请输入正确的system')

    file_name = args.file_name
    version = args.version
    number = args.number

    thread_run(token=MONKEY_TOKEN_TEST, file_name=file_name, system=system, project=TEST_PROJECT, version=version, number=number)

    










