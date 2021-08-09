"""
@Project : 爬虫-无锡预决算审查
@Author  : huangdong
@Data    : 2021/7/31 0:00
@File    : wuxi_bugdets_finalAccounts
@Software: PyCharm
@Declare :
"""
import os
import json
import datetime

import requests
import pandas as pd

# 异步方法https://www.cnblogs.com/jiyongjia/p/9803991.html
# http://www.jszwfw.gov.cn/yjsgk/list.do
list_mul = []

url_1 = 'http://www.jszwfw.gov.cn/yjsgk/front/department/getdepartmentname.do'
# url_1 = 'http://yjsgk.jsczt.cn/front/department/getdepartmentname.do'
header_1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
}

header_2 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
}

start_time = datetime.datetime.now()
str_now = datetime.datetime.now().strftime(u'%Y%m%d-%H{h}%M{m}').format(h='时', m='分')
str_dirName = f'预算{str_now}'

if not os.path.exists(str_dirName):
    os.makedirs(str_dirName)

dict_district_info = {
    '无锡市': 'pid=122&departmentName=&date=2021&typeid=2',
    '惠山区': 'pid=160&departmentName=&date=2021&typeid=2',
    '锡山区': 'pid=159&departmentName=&date=2021&typeid=2',
    '滨湖区': 'pid=161&departmentName=&date=2021&typeid=2',
    '梁溪区': 'pid=162&departmentName=&date=2021&typeid=2',
    '新吴区': 'pid=163&departmentName=&date=2021&typeid=2',
    '江阴市': 'pid=164&departmentName=&date=2021&typeid=2',
    '宜兴市': 'pid=165&departmentName=&date=2021&typeid=2',
}
for district in dict_district_info:
    resp = requests.post(url_1, headers=header_1, data=dict_district_info[district])
    info = resp.text
    print(info)
    data = json.loads(info)
    # print(type(data))
    for i in data:
        iid = i['iid']
        name = i['name']
        pid = i['pid']
        dtype = i['dtype']
        print(district, name)
        print(iid, pid, dtype)
        # url_depart = \
        # f'http://www.jszwfw.gov.cn/yjsgk/front/glwj/list.do?groupid={pid}&channel={dtype}&date=2021&bmid={iid}'
        url_depart = 'http://yjsgk.jsczt.cn/front/budgetfinal/itemsandpag.do'
        data_depart = f'page_num=1&groupid={iid}&typeid=2&date=2021'
        r = requests.post(url_depart, headers=header_1, data=data_depart)
        cont_depart = json.loads(r.text)
        uuid = cont_depart['budgetTemplates'][0]['uuid']
        print(uuid)

        url_file_info = 'http://yjsgk.jsczt.cn/front/budgetfinal/getTemporaryFiles.do'
        data_depart_2 = f"uuid='{uuid}'"
        r = requests.post(url_file_info, headers=header_1, data=data_depart_2)
        data_info = json.loads(r.text)

        cc = len(data_info)
        if cc > 1:
            dict_i = {
                '地区': district,
                '部门': name,
                '附件个数': cc
            }
            list_mul.append(dict_i)
            print(cc, '&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
        count = 0
        for mm in data_info:
            count += 1
            file_name = mm['t_oldname']
            iid_2 = mm['iid']
            uploaddate = mm['t_uploaddate']
            depart = name
            url_download = f'http://yjsgk.jsczt.cn/front/budgetfinal/download.do?iid={iid_2}'
            resp = requests.get(url_download, headers=header_2)
            with open(f'./{str_dirName}/{district}_{depart}_{iid}_{count}_{uploaddate}_{file_name}', 'ab') as f:
                f.write(resp.content)

df = pd.DataFrame(list_mul)
df.to_excel(f'预算{str_now}.xlsx', index=False)

end_time = datetime.datetime.now()

print(end_time - start_time, '耗时')
