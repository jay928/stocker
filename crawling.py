import requests
import re
import numpy as np

from bs4 import BeautifulSoup

DATE = 'DATE'
FIRST = 'FIRST'
FINAL = 'FINAL'
HIGHEST = 'HIGHEST'
LOWEST = 'LOWEST'
VOLUME = 'VOLUME'

SAVE_PATH = '/Volumes/SD/deeplearning/data/stock/'


def checkHttpError(res):
    if res.status_code != 200:
        print("ERROR (code:", res.status_code, ")")
        return False
    return True


def getPriceUrl(code, page):
    url = 'https://finance.naver.com/item/sise_day.nhn?code='
    url += str(code)
    url += '&page='
    url += str(page)
    return url


def getAllUrl(code):
    url = 'https://finance.naver.com/item/sise.nhn?code='
    url += str(code)
    return url


def getFoAgUrl(code, page):
    url = 'https://finance.naver.com/item/frgn.nhn?code='
    url += str(code)
    url += '&page='
    url += str(page)
    return url


def getExistsUrl(code):
    url = 'https://finance.naver.com/item/main.nhn?code='
    url += str(code)
    return url


def requestAndGetSoup(url):
    response = requests.get(url)
    if checkHttpError(response) is False:
        return None
    return BeautifulSoup(response.text, 'html.parser')


def pickTitle(soup):
    title = soup.find('title').get_text()
    return title[0:title.index(":") - 1]


def pickTotalVolume(soup):
    digits = re.findall("\d+",
                        soup.find('em', id='_market_sum').get_text().replace(',', '').replace(' ', '').replace('\t',
                                                                                                               '').replace(
                            '\n', '').encode('UTF-8'))
    digitString = ''
    for digit in digits:
        digitString += str(digit)

    return int(digitString)


def pickData(soup):
    pickedData = []

    tr_soup = soup.find_all('tr')
    for tr_idx in range(len(tr_soup)):
        td_soup = tr_soup[tr_idx].find_all('td')
        if len(td_soup) != 7:
            continue

        rowDir = {}
        td_soup = tr_soup[tr_idx].find_all('td')

        t = td_soup[0].get_text().replace('.', '')
        if t.isdecimal() is not True:
            continue

        for td_idx in range(len(td_soup)):
            if td_idx == 0:
                rowDir[DATE] = int(td_soup[td_idx].get_text().replace('.', ''))
            elif td_idx == 1:
                rowDir[FINAL] = int(td_soup[td_idx].get_text().replace(',', ''))
            elif td_idx == 3:
                rowDir[FIRST] = int(td_soup[td_idx].get_text().replace(',', ''))
            elif td_idx == 4:
                rowDir[HIGHEST] = int(td_soup[td_idx].get_text().replace(',', ''))
            elif td_idx == 5:
                rowDir[LOWEST] = int(td_soup[td_idx].get_text().replace(',', ''))
            elif td_idx == 6:
                rowDir[VOLUME] = int(td_soup[td_idx].get_text().replace(',', ''))

        pickedData.append(rowDir)

    return pickedData


def pickData2(soup):
    pickedData = []

    tr_soup = soup.find_all('tr')
    for tr_idx in range(len(tr_soup)):
        td_soup = tr_soup[tr_idx].find_all('td')
        if len(td_soup) != 9:
            continue

        rowDir = {}
        td_soup = tr_soup[tr_idx].find_all('td')
        for td_idx in range(len(td_soup)):
            if td_idx == 0:
                rowDir[DATE] = int(td_soup[td_idx].get_text().replace('.', ''))
            elif td_idx == 5:
                rowDir[AGENCY] = int(td_soup[td_idx].get_text().replace(',', ''))
            elif td_idx == 6:
                rowDir[FOREIGN] = int(td_soup[td_idx].get_text().replace(',', ''))

        pickedData.append(rowDir)

    return pickedData


def insert(conn, codeName, code, totalVolume, basicDate, firstPrice, finalPrice, highestPrice, lowestPrice, volume):
    curs = conn.cursor()
    sql = """insert into stock(
             codeName, code, totalVolume, basicDate, firstPrice, finalPrice, highestPrice, lowestPrice, volume, createdAt
             )
             values (
             %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
             )"""

    curs.execute(sql,
                 (codeName, code, totalVolume, basicDate, firstPrice, finalPrice, highestPrice, lowestPrice, volume))
    conn.commit()


def close(conn):
    if conn is not None:
        conn.close()


def updateFoAg(conn, code, date, foreign, agency):
    curs = conn.cursor()
    sql = """update stock set foreign = %s, agency = %s where basicDate = %s and code  = %s)"""
    curs.execute(sql, (foreign, agency, date, code))
    conn.commit()


def selectLastDate(conn, code):
    curs = conn.cursor()

    sql = "select * from stock where code = %s order by basicDate desc limit 1"
    curs.execute(sql, (code))

    rows = curs.fetchall()
    return rows[0]['basicDate']


def selectExist(conn, code, targetDate):
    curs = conn.cursor()11

    sql = "select count(1) as cnt from stock where code = %s and basicDate = %s"
    curs.execute(sql, (code, targetDate))

    rows = curs.fetchall()
    return rows[0][0] > 0


def saveStockCodes(num):
    stockCodes = []

    for codeNumber in range(1 + (num*100000), (num*100000) + 100000):
        code = ''
        for idx in range(len(str(codeNumber)), 6):
            code += '0'
        code += str(codeNumber)

        try:
            tsoup = requestAndGetSoup(getExistsUrl(code))
        except:
            continue

        if '<!-- ERROR -->' in str(tsoup):
            print("ERROR : " + code)
        else:
            stockCodes.append(code)
            print("SAVED : " + code)

    np.save(SAVE_PATH + 'codes' + str(num), stockCodes, allow_pickle=True)



def execute(start, end, startPage, endPage):
    stockData = {}

    for codeNumber in range(start, end):
        code = ''
        for idx in range(len(str(codeNumber)), 6):
            code += '0'
        code += str(codeNumber)

        print(code)

        tsoup = requestAndGetSoup(getAllUrl(code))
        if tsoup is None:
            continue

        title = pickTitle(tsoup).encode('UTF-8')
        if title is None or title == '':
            continue

        vsoup = requestAndGetSoup(getAllUrl(code))
        if vsoup is None:
            continue

        # totalVolume = pickTotalVolume(vsoup)

        for idx in range(startPage, endPage+1):
            dsoup = requestAndGetSoup(getPriceUrl(code, idx))
            datas = pickData(dsoup)

            for data in datas:
                date = str(data[DATE])
                if stockData.get(date) is None:
                    stockData[date] = []

                stockData[date].append([code, data[FIRST], data[FINAL], data[HIGHEST], data[LOWEST], data[VOLUME]])

    for k, v in stockData:
        np.save(SAVE_PATH + k, v, allow_pickle=True)


for i in range(1, 10):
    saveStockCodes(i)
# execute(0, 999999, 1, 10)

