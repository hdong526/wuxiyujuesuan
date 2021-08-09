"""
@Project : 爬虫-无锡预决算审查
@Author  : huangdong
@Data    : 2021/8/2 16:41
@File    : wuxi_finalAccounts_multhreads
@Software: PyCharm
@Declare :
"""
import os
import json
import copy
import queue
import datetime
import threading
from os.path import dirname, abspath, join
# from urllib3.exceptions import NewConnectionError
from requests.exceptions import RequestException

import requests
import pandas as pd
from loguru import logger
from retrying import retry


#  http://www.jszwfw.gov.cn/yjsgk/list.do
header_1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
}

header_2 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
}

Q = queue.Queue()
list_files = []
S = requests.Session()

ROOT_DIR = dirname(dirname(abspath(__file__)))
LOG_DIR = join(ROOT_DIR, '爬虫-无锡预决算审查/logs')
logger.add(
    join(LOG_DIR, 'runtime.log'),
    level='DEBUG',
    rotation='1 week',
    retention='20 days',
)

logger.add(
    join(LOG_DIR, 'error.log'),
    level='ERROR',
    rotation='1 week'
)


@retry(stop_max_attempt_number=3, retry_on_result=lambda x: x is None, wait_fixed=2000)
def fetch(url, fetch_type='GET', content_type='TEXT', **kwargs):
    try:
        kwargs.setdefault('timeout', 30)
        kwargs.setdefault('verify', False)
        if fetch_type == 'GET':
            response = S.get(url, **kwargs)
        else:
            response = S.post(url, **kwargs)
        if response.status_code == 200:
            if content_type == 'TEXT':
                response.encoding = 'utf-8'
                return response.text
            else:
                return response.content
    except requests.ConnectionError:
        return


def spider_step_1(district_info):
    district = list(district_info.keys())[0]
    district_data = district_info[district]
    logger.info(district)
    logger.info(district_data)
    url_1 = 'http://www.jszwfw.gov.cn/yjsgk/front/department/getdepartmentname.do'
    requests_info = {
        'headers': header_1,
        'data': district_data,
    }
    info = fetch(url_1, 'POST', **requests_info)
    data = json.loads(info)
    for i in data:
        iid = i['iid']
        name = i['name']
        pid = i['pid']
        dtype = i['dtype']
        logger.info(f'{district}{name}')
        logger.info(f'{iid}{pid}{dtype}')
        Q.put({
            'step_2': {
                '地区': district,
                '部门': name,
                'iid': iid,
            }
        })


def spider_step_2(step_2_info):
    iid = step_2_info['iid']
    url_depart = 'http://yjsgk.jsczt.cn/front/budgetfinal/itemsandpag.do'
    data_depart = f'page_num=1&groupid={iid}&typeid=2&date=2021'
    requests_info = {
        'headers': header_1,
        'data': data_depart,
    }
    info = fetch(url_depart, 'POST', **requests_info)
    cont_depart = json.loads(info)
    uuid = cont_depart['budgetTemplates'][0]['uuid']
    step_2_info['uuid'] = uuid
    Q.put({'step_3': step_2_info})


def spider_step_3(step_3_info):
    uuid = step_3_info['uuid']
    url_file_info = 'http://yjsgk.jsczt.cn/front/budgetfinal/getTemporaryFiles.do'
    data_depart_2 = f"uuid='{uuid}'"
    requests_info = {
        'headers': header_1,
        'data': data_depart_2,
    }
    info = fetch(url_file_info, 'POST', **requests_info)
    data_info = json.loads(info)
    cc = len(data_info)
    if cc > 1:
        step_3_info['附件个数'] = cc
        list_files.append(step_3_info)
        logger.info(f'{cc}&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
    count = 0
    for mm in data_info:
        count += 1
        step_info = copy.deepcopy(step_3_info)
        step_info['file_name'] = mm['t_oldname']
        step_info['iid_2'] = mm['iid']
        step_info['uploaddate'] = mm['t_uploaddate']
        step_info['count'] = count
        Q.put({'step_4': step_info})


def spider_step_4(step_4_info, dir_path):
    count = step_4_info['count']
    file_name = step_4_info['file_name']
    iid_2 = step_4_info['iid_2']
    uploaddate = step_4_info['uploaddate']
    depart = step_4_info['部门']
    district = step_4_info['地区']
    iid = step_4_info['iid']
    url_download = f'http://yjsgk.jsczt.cn/front/budgetfinal/download.do?iid={iid_2}'
    requests_info = {
        'headers': header_2,
    }
    info = fetch(url_download, 'GET', 'CONTENT', **requests_info)
    with open(f'./{dir_path}/{district}_{depart}_{iid}_{count}_{uploaddate}_{file_name}', 'ab') as f:
        f.write(info)


@logger.catch
def run(dir_path):
    while True:
        q_info = Q.get()
        try:
            func_type = list(q_info.keys())[0]
            if func_type == 'step_1':
                spider_step_1(q_info[func_type])
            elif func_type == 'step_2':
                spider_step_2(q_info[func_type])
            elif func_type == 'step_3':
                spider_step_3(q_info[func_type])
            elif func_type == 'step_4':
                spider_step_4(q_info[func_type], dir_path)
            else:
                pass
            Q.task_done()
        except RequestException as e:
            Q.task_done()
            Q.put(q_info)
            logger.error(str(e))
            logger.error(f'{q_info}error,error,error,error,error,error,error,error')


def main():
    str_now = datetime.datetime.now().strftime(u'%Y%m%d-%H{h}%M{m}').format(h='时', m='分')
    str_dirname = f'预算{str_now}'

    if not os.path.exists(str_dirname):
        os.makedirs(str_dirname)

    dict_district_info = [
        {'无锡市': 'pid=122&departmentName=&date=2021&typeid=2'},
        {'惠山区': 'pid=160&departmentName=&date=2021&typeid=2'},
        {'锡山区': 'pid=159&departmentName=&date=2021&typeid=2'},
        {'滨湖区': 'pid=161&departmentName=&date=2021&typeid=2'},
        {'梁溪区': 'pid=162&departmentName=&date=2021&typeid=2'},
        {'新吴区': 'pid=163&departmentName=&date=2021&typeid=2'},
        {'江阴市': 'pid=164&departmentName=&date=2021&typeid=2'},
        {'宜兴市': 'pid=165&departmentName=&date=2021&typeid=2'},
    ]
    [Q.put({'step_1': district}) for district in dict_district_info]
    return str_dirname, str_now


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    dir_name, str_time = main()
    list_threads = [threading.Thread(target=run, args=(dir_name, )) for i in range(12)]
    [thread_run.setDaemon(True) for thread_run in list_threads]
    [thread_run.start() for thread_run in list_threads]
    Q.join()
    df = pd.DataFrame(list_files)[['地区', '部门', '附件个数']]
    df.to_excel(f'预算{str_time}.xlsx', index=False)
    logger.info('queue is empty now')
    # [thread_run.join() for thread_run in list_threads]
    end_time = datetime.datetime.now()
    logger.info(str(end_time - start_time), '耗时')
