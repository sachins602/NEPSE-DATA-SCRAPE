import requests
import json
import mysql.connector

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80'
}
sectorss = { 'corporate_debentures':['NICAD8283', 'NBLD85'],
            'microfinance':['ACLBSL','ALBSL','CBBL','CLBSL','DDBL','FMDBL','FOWAD','GMFBS','GILB','GBLBS','GLBSL','ILBS','JALPA','JSLBB','JBLB','KMCDB','KLBSL','LLBS','MLBSL','MSLB','MKLB','MLBS','MERO','MMFDB','MLBBL','NSLB','NLBBL','NESDO','NICLBSL','NUBL','RULB','RMDC','RSDC','SABSL','SDLBSL','SMATA','SLBSL','SKBBL','SMFDB','SMB','SWBBL','SMFBS','SLBBL','USLB','VLBS','WNLB'],
            'commercial_banks':['ADBL','BOKL','CCBL','CZBIL','CBL','EBL','GBIME','KBL','LBL','MBL','MEGA','NABIL','NBL','NCCB','SBI','NICA','NMB','PRVU','PCBL','SANIMA','SBL','SCB','SRBL'],
            'non_life_insurance':['AIL','EIC','GIC','HGI','IGI','LGIL','NIL','NICL','NLG','PRIN','PIC','PICL','RBCL','SIC','SGI','SICL','SIL','UIC'],
            'hydro_powers':['AKJCL','API','AKPL','AHPC','BARUN','BNHC','BPCL','CHL','CHCL','DHPL','GHL','GLH','HDHPC','HURJA','HPPL','JOSHI','KPCL','KKHC','LEC','MBJC','MKJC','MEN','MHNL','NHPC','NHDL','NGPL','NYADI','PMHPL','PPCL','RADHI','RHPL','RURU','SAHAS','SPC','SHPC','SJCL','SSHL','SHEL','SPDL','TPC','UNHPL','UMRH','UMHL','UPCL','UPPER'],
            'life_insurance':['ALICL','GLICL','JLI','LICN','NLICL','NLIC','PLI','PLIC','RLI','SLI','SLICL','ULI'],
            'finance':['BFC','CFCL','GFCL','GMFIL','GUFL','ICFC','JFL','MFIL','MPFL','NFS','PFL','PROFL','RLFL','SFCL','SIFC'],
            'tradings':['BBC', 'STC'],
            'manufacturing_and_processing':['BNT', 'HDL', 'SHIVM', 'UNL'],
            'investment':['CHDC', 'CIT', 'ENL', 'HIDCL', 'NIFRA', 'NRN'],
            'hotels':['CGH', 'OHL', 'SHL', 'TRH'],
            'development_banks':['CORBL','EDBL','GBBL','GRDBL','JBBL','KSBBL','KRBL','LBBL','MLBL','MDB','MNBBL','NABBC','SAPDBL','SADBL','SHINE','SINDU'],
            'mutual_fund':['KEF','LUK','NEF','NIBLPF'],
            'other':['NTC', 'NRIC']
           }


def historic_data_populator(sectors):
    for item in sectors.items():
        sector_name = item[0]
        company_list = item[1]
        for name in company_list:
            scrip = name
            data_list = []
            start_time = 1325376000  # 2012 january 1
            end_time = 1656547200
            response = requests.get(
                'https://nepsealpha.com/trading/0/history?symbol=' + scrip + '&resolution=1D&from=' + str(
                    start_time) + '&to=' + str(end_time) + '&pass=ok&force=0.01918733810602169&currencyCode=NRS',headers=headers)
            a = response.json()
            Open = a['o']
            High = a['h']
            Close = a['c']
            Low = a['l']
            Volume = a['v']
            Time = a['t']

            database_name = 'stock' ########## change according to need ########

            db = mysql.connector.connect(host='localhost', user='root', password='', database=database_name)
            cursor = db.cursor()
            for o,h,c,l,v,t in zip(Open,High,Close,Low,Volume,Time):
                data = (scrip,t,c,o,h,l,v)
                data_list.append(data)
            sql_query = 'INSERT INTO  '+sector_name+f' (Scrip, Time, Close, Open, High, Low, Volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'

            cursor.executemany(sql_query,data_list)
            db.commit()
    cursor.close()
    db.close()
historic_data_populator(sectorss)







