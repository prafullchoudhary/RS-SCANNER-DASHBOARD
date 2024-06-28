from bs4 import BeautifulSoup
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from asyncio import Semaphore

async def scrape_website(url, semaphore):
    async with semaphore:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url, wait_until='networkidle',timeout=0)
            content = await page.content()
            await browser.close()
            return content

async def scrape_website_sector(url, semaphore):
    df = pd.DataFrame(columns=['Sector', 'Comapny_Name', 'NSE_Symbol', 'MCAP'])
    content = await scrape_website(url, semaphore)  # Limit to 10 concurrent tasks
    soup = BeautifulSoup(content, 'html.parser')
    sector=soup.find('h1').text.replace("/", "_")
    table = soup.find("table", {'id': 'companylist'})
    com_links = table.find_all("a")
    rows=table.find_all('tr')
    i=0
    for l in com_links:
        i+=1
        mcap=float((rows[i].find_all('td'))[4]['value'])
        if mcap>300:
            if (l['href']).find("SCRIP-") == -1:
                df.loc[i] = {'Sector': sector, 'Comapny_Name': l.text, 'NSE_Symbol': l['href'][9:], 'MCAP': mcap}
    # print(df.sort_values(by='MCAP', ascending=False))
    print(l.text)
    return df.sort_values(by='MCAP', ascending=False)

async def main():
    df = pd.DataFrame(columns=['Sector', 'Comapny_Name', 'NSE_Symbol', 'MCAP'])
    url = 'https://ticker.finology.in'
    surl = 'https://ticker.finology.in/sector'
    content = await scrape_website(surl, Semaphore(5))  # Limit to 10 concurrent tasks
    soup = BeautifulSoup(content, 'html.parser')
    sectors_links = soup.find_all("a", {'class': 'btn btn-sm btn-primary align-self-center'})
    semaphore = Semaphore(5)  # Limit to 10 concurrent tasks
    tasks = []
    for slink in sectors_links:
        task = asyncio.ensure_future(scrape_website_sector(url + slink['href'], semaphore))
        tasks.append(task)
    await asyncio.gather(*tasks)
    for task in tasks:
        df = pd.concat([df,task.result()],ignore_index=True)
    df.to_csv('scrap.csv',index=False)
                

asyncio.run(main())

# def url_path_contains_scrip(url):
#     path = urlparse(url).path
#     return 'SCRIP-' in path

# async def scrape_website_company(url, semaphore):
#     return await scrape_website(url, semaphore)

# clink=url + l['href']
                # if url_path_contains_scrip(clink)==False:
                #     print(sector,type(clink),clink)
                    # ctask = asyncio.ensure_future(scrape_website_company(clink, semaphore))
                    # ctasks.append(ctask)
    # print(datetime.now().time())
    # await asyncio.gather(*ctasks)
    # print(datetime.now().time())
    # for ctask in ctasks:
    #     c_soup = BeautifulSoup(await ctask, 'html.parser')
    #     cname = c_soup.find("span", {'id': 'mainContent_ltrlCompName', 'class': 'h1 font-weight-bold'}).text
    #     indices = c_soup.find("p", {'id': 'mainContent_compinfoId', 'class': 'compinfo sector mt-1'})
    #     if (indices.text).find("NSE") != -1:
    #         nse_symbol = indices.find('strong').text
    #         sector = indices.find('a').text
    #         essential_t = c_soup.find('div', {'id': "mainContent_updAddRatios", 'class': 'row no-gutters'})
    #         mcap = float((essential_t.find('span', {'class': 'Number'}).text).replace(',', ''))
    #         if mcap > 300:
    #             df.loc[i] = {'Sector': sector, 'Comapny_Name': cname, 'NSE_Symbol': nse_symbol, 'MCAP': mcap}
    #             print(df.iloc[-1])
    #             i += 1
