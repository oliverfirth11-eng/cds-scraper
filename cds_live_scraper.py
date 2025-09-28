import asyncio 
import aiohttp
import pandas as pd
import psycopg2
import zipfile
import io
import re
import logging
from datetime import datetime, time
from bs4 import BeautifulSoup
import os

class CDSRealTimeScraper:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        self.dtcc_slice_url = "https://pddata.dtcc.com/ppd/cftcdashboard"
        self.target_entities = {
            'VODAFONE GROUP PLC': 'VODFON',
            'DEUTSCHE BANK AG': 'DBKGN',
            'BNP PARIBAS SA': 'BNPPR',
            'BANCO SANTANDER SA': 'SANES',
            'TELECOM ITALIA SPA': 'TITIM',
            'TOTALENERGIES SE': 'TOTFP',
            'SHELL PLC': 'SHEL',
            'SIEMENS AG': 'SIE',
            'BMW AG': 'BMW',
            'SAP SE': 'SAP',
        }
        self.processed_trades = set()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def get_latest_credit_slice(self):
        async with aiohttp.ClientSession() as session:
            resp = await session.get(self.dtcc_slice_url)
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            links = soup.find_all("a", href=re.compile(r"CFTC_SLICE_CREDITS.*\.ZIP"))
            if not links:
                logging.warning("No credit slice files found.")
                return None
            latest_zip_url = links[0]["href"]
            resp = await session.get(latest_zip_url)
            data = await resp.read()
            with zipfile.ZipFile(io.BytesIO(data)) as zipf:
                csv_name = zipf.namelist()[0]
                with zipf.open(csv_name) as csvfile:
                    df = pd.read_csv(csvfile)
                    logging.info(f"Downloaded {csv_name} with {len(df)} trades")
                    return df

    def filter_eur_cds(self, df):
        if df is None or df.empty:
            return pd.DataFrame()
        filtered = df[
            (df["Asset Class"] == "CR") & 
            (df["Notional currency-Leg 1"] == "EUR") & 
            (df["Underlying Asset Name"].isin(self.target_entities.keys()))
        ]
        logging.info(f"Filtered {len(filtered)} European CDS trades")
        return filtered

    def format_notional(self, amount):
        if amount >= 1e6:
            return f"{amount/1e6:.3f}M"
        if amount >= 1e3:
            return f"{amount/1e3:.1f}K"
        return str(int(amount))

    def process_trades(self, df):
        processed = []
        now = pd.Timestamp.now()
        for _, row in df.iterrows():
            trade_id = str(row.get("Dissemination Identifier"))
            if trade_id in self.processed_trades:
                continue
            underlying = row.get("Underlying Asset Name")
            ticker = self.target_entities.get(underlying, underlying[:6])
            notional = float(row.get("Notional amount-Leg 1", 0))
            instr_time = pd.to_datetime(row.get("Execution Timestamp")).time()
            expiry = pd.to_datetime(row.get("Expiration Date"))
            years_to_maturity = max(1, (expiry - now).days // 365)
            instrument = f"{ticker} CDS EUR SR {years_to_maturity}Y"
            price = round(float(row.get("Price", 0)), 2)
            platform_id = row.get("Platform identifier", "")[:4]
            other_payment = row.get("Other payment amount")
            processed.append({
                "trade_time": instr_time,
                "instrument": instrument,
                "price": price,
                "notional_display": self.format_notional(notional),
                "code": "TR",
                "maturity_date": expiry.strftime("%d/%m/%y"),
                "rate_spread": years_to_maturity,
                "currency": "EUR",
                "notional_full": int(notional),
                "platform_id": platform_id,
                "other_payment": float(other_payment) if pd.notnull(other_payment) else None,
                "rating_category": "HY" if ticker == "TITIM" else "IG",
                "entity_name": underlying,
                "sector": "",
            })
            self.processed_trades.add(trade_id)
        return processed

    async def save_trades(self, trades):
        if not trades:
            logging.info("No new trades to save")
            return
        conn = psycopg2.connect(self.database_url, sslmode='require')
        cur = conn.cursor()
        for t in trades:
            cur.execute("""
            INSERT INTO cds_trades_live (
                trade_time, instrument, price, notional_display, code,
                maturity_date, rate_spread, currency, notional_full,
                platform_id, other_payment, rating_category, entity_name, sector
            ) VALUES (%(trade_time)s, %(instrument)s, %(price)s, %(notional_display)s, %(code)s,
                      %(maturity_date)s, %(rate_spread)s, %(currency)s, %(notional_full)s,
                      %(platform_id)s, %(other_payment)s, %(rating_category)s, %(entity_name)s, %(sector)s)
            ON CONFLICT DO NOTHING
            """, t)
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Saved {len(trades)} trades to database")

    async def run(self):
        while True:
            logging.info("Starting scrape cycle")
            df = await self.get_latest_credit_slice()
            eur_cds = self.filter_eur_cds(df)
            trades = self.process_trades(eur_cds)
            await self.save_trades(trades)
            logging.info("Cycle complete, sleeping 1 minute")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(CDSRealTimeScraper().run())
  
