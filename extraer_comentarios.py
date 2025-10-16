import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
     # Links generales pauta QR Facebook & Tik Tok
    "https://www.tiktok.com/@alpinacol/video/7545906205563112722?_r=1&_t=ZS-8zodFWYH3fr",
    "https://www.tiktok.com/@alpinacol/video/7545906164911934736?_r=1&_t=ZS-8zodC7ZCI5a",
    "https://www.tiktok.com/@alpinacol/video/7545906204736900359?_r=1&_t=ZS-8zod8bavifI",
    "https://www.tiktok.com/@alpinacol/video/7545906203931757831?_r=1&_t=ZS-8zod3GQAOL2",
    "https://www.tiktok.com/@alpinacol/video/7545783917819858183?_r=1&_t=ZS-8zoczvlumrX",
    "https://www.facebook.com/share/v/1AECsaNCAg/",
    "https://www.facebook.com/share/r/19tQheWQPf/",
    "https://www.facebook.com/share/r/1CJf8Y6ZQ3/",
    "https://www.facebook.com/share/r/16dwuwzUGy/",
    "https://www.facebook.com/share/r/17Pta7RTLQ/",
     # Links adicionales Pauta 2025
    "https://www.instagram.com/p/DOJ1IfZgMkK/",
    "https://www.instagram.com/p/DOKy5xrgAWJ/",
    "https://www.instagram.com/p/DOKy5uTgKUO/",
    "https://www.instagram.com/p/DOKy5PLADHq/",
    "https://www.instagram.com/p/DOKy5cEAAcQ/",
    "https://www.instagram.com/p/DOKy5ejgFaV/",
    "https://www.instagram.com/p/DOJ1Iv0ABvo/",
    "https://www.instagram.com/p/DOKy5sBAAWP/",
    "https://www.instagram.com/p/DOJ1IvXAI4e/",
    "https://www.instagram.com/p/DOJ1I3bACvH/",
    "https://www.instagram.com/p/DOKy5fiALE9/",
    "https://www.instagram.com/p/DOKy5rdAPK6/",
    "https://www.instagram.com/p/DOJ1I-ngOqh/",
    "https://www.instagram.com/p/DOKy5iDAIdy/",
    "https://www.instagram.com/p/DOJk2bpgKSF/",
    "https://www.instagram.com/p/DOKy589gPp1/",
    "https://www.instagram.com/p/DOKy5iQANz7/",
    "https://www.instagram.com/p/DOKy50FgF6j/",
    "https://www.instagram.com/p/DOJ1I-8AJtl/",
    "https://www.instagram.com/p/DOKy5q3AKsY/",
    "https://www.instagram.com/p/DOJk2MUgE8f/",
    "https://www.instagram.com/p/DOKy5WUAEwC/",
    "https://www.instagram.com/p/DOKy5uwgBs0/",
    "https://www.instagram.com/p/DOKy5aQgLAr/",
    "https://www.instagram.com/p/DOJ1I0TgLBE/",
    "https://www.instagram.com/p/DOJk1DsgF9F/",
    "https://www.instagram.com/p/DOJ1IKDAN8A/",
    "https://www.instagram.com/p/DOKy5jMgKDI/",
    "https://www.instagram.com/p/DOKy5gcAHFr/",
    "https://www.instagram.com/p/DOJ1JAhgH2b/",
    "https://www.instagram.com/p/DOKy5orgCHj/",
    "https://www.instagram.com/p/DOKy5k-AOiR/",
    "https://www.instagram.com/p/DOJ1I3AAM4q/",
    "https://www.instagram.com/p/DOKy5a4AEX0/",
    "https://www.instagram.com/p/DOJk2mAACPq/",
    "https://www.instagram.com/p/DOJ1KKDANI7/",
    "https://www.instagram.com/p/DOKy51xAIei/",
    "https://www.instagram.com/p/DOJh3vLgPqb/",
    "https://www.instagram.com/p/DOKy5eYAHJn/",
    "https://www.instagram.com/p/DOKy6OrACfT/",
    "https://www.instagram.com/p/DOJ1HuCgM5g/",
    "https://www.instagram.com/p/DOKy5fJAIr4/",
    "https://www.instagram.com/p/DOKy5dDAGfg/",
    "https://www.instagram.com/p/DOJk25pgKiT/",
    #"https://www.facebook.com/?feed_demo_ad=120234317977680528&h=AQBvZPv-3bvs8SJ8byc",
    #"https://www.facebook.com/?feed_demo_ad=120234198744780528&h=AQAEu-V1J3rvOBnhk_A",
    #"https://www.facebook.com/?feed_demo_ad=120234237928540528&h=AQArOkfYaG2w25Jo2vA",
    #"https://www.facebook.com/?feed_demo_ad=120234199096330528&h=AQALGqizsR8GVZkEem4",
    #"https://www.facebook.com/?feed_demo_ad=120234237275450528&h=AQCo3u5swn6A3SBi4-E",
    #"https://www.facebook.com/?feed_demo_ad=120234197141210528&h=AQDc1IVqYETXIgYP1Ak",
    #"https://www.facebook.com/?feed_demo_ad=120234316601330528&h=AQDGAnMh7qqDyK_yWtI",
    #"https://www.facebook.com/?feed_demo_ad=120234198421170528&h=AQB06GHBgdqkAB8GJT4",
    #"https://www.facebook.com/?feed_demo_ad=120234310003560528&h=AQB9GvIGBtCsB3RxO6A",
    #"https://www.facebook.com/?feed_demo_ad=120234317015340528&h=AQDrOMxQfgBdwsaP7yo",
    #"https://www.facebook.com/?feed_demo_ad=120234241726820528&h=AQAD2ZJjVjkB8WdeP4E",
    #"https://www.facebook.com/?feed_demo_ad=120234237653380528&h=AQAwtptR2E9Epipk_5M",
    #"https://www.facebook.com/?feed_demo_ad=120234318125910528&h=AQAh5kI5WMn5K0ghXM4",
    #"https://www.facebook.com/?feed_demo_ad=120234238170860528&h=AQDLsgawJjxAfF0XcOY",
    #"https://www.facebook.com/?feed_demo_ad=120234308557670528&h=AQDHPyIi3-EcN8vrEnE",
    #"https://www.facebook.com/?feed_demo_ad=120234241334590528&h=AQDk3i1Hb_Ndw9Td9J0",
    #"https://www.facebook.com/?feed_demo_ad=120234237737840528&h=AQBFfiHPf2FCqee3C0M",
    #"https://www.facebook.com/?feed_demo_ad=120234239144160528&h=AQBMZm5FiF2IYdvN8wE",
    #"https://www.facebook.com/?feed_demo_ad=120234309959970528&h=AQBmCbg77rT0HheZoJ8",
    #"https://www.facebook.com/?feed_demo_ad=120234240744960528&h=AQDyqwsjA_tIi-xmR0w",
    #"https://www.facebook.com/?feed_demo_ad=120234307839770528&h=AQBQuJ8O86vFkIcgsbo",
    #"https://www.facebook.com/?feed_demo_ad=120234241676350528&h=AQC6W3KtNDnAQO5TZQA",
    #"https://www.facebook.com/?feed_demo_ad=120234241803710528&h=AQAsYfbv6eFAHMAHeSo",
    #"https://www.facebook.com/?feed_demo_ad=120234236557040528&h=AQBh66jw5evUpb9gt9c",
    #"https://www.facebook.com/?feed_demo_ad=120234317914330528&h=AQD9C7aypz5aW-atXyM",
    #"https://www.facebook.com/?feed_demo_ad=120234308967340528&h=AQAMCUJO8WV81nVE3_Q",
    #"https://www.facebook.com/?feed_demo_ad=120234309744400528&h=AQB_AVy55ncEONWuaGQ",
    #"https://www.facebook.com/?feed_demo_ad=120234239022710528&h=AQBMLe6aDbqPF_T1Ovo",
    #"https://www.facebook.com/?feed_demo_ad=120234198935170528&h=AQBB9WlK7cqn3hjaqPw",
    #"https://www.facebook.com/?feed_demo_ad=120234318260400528&h=AQAyU7lKiDfigdtRLHw",
    #"https://www.facebook.com/?feed_demo_ad=120234236312470528&h=AQARTOPjewPSQmFb8wo",
    #"https://www.facebook.com/?feed_demo_ad=120234238361610528&h=AQDCnf8yAtLks4Dyz5k",
    #"https://www.facebook.com/?feed_demo_ad=120234309788000528&h=AQANi1kfHrRIadN6onA",
    #"https://www.facebook.com/?feed_demo_ad=120234239233720528&h=AQCE0OoUgwgjExRHpZk",
    #"https://www.facebook.com/?feed_demo_ad=120234308798670528&h=AQBQ31Qea_ObC9c7TVc",
    #"https://www.facebook.com/?feed_demo_ad=120234317817200528&h=AQDI1uP9lj-YBkJNPsE",
    #"https://www.facebook.com/?feed_demo_ad=120234239536130528&h=AQCioJA53g0RXqVBuE0",
    #"https://www.facebook.com/?feed_demo_ad=120234307377020528&h=AQDvBc9yyQ2YZoogNAM",
    #"https://www.facebook.com/?feed_demo_ad=120234236454060528&h=AQA6JM0XvTspvU3mYgM",
    #"https://www.facebook.com/?feed_demo_ad=120234239394020528&h=AQBk3P59dpuDyRu3YMA",
    #"https://www.facebook.com/?feed_demo_ad=120234318359350528&h=AQDRn6fUXhWQMHYUDhY",
    #"https://www.facebook.com/?feed_demo_ad=120234237478820528&h=AQDc0-O4SWkHOv2pgSQ",
    #"https://www.facebook.com/?feed_demo_ad=120234237827000528&h=AQBHxEEBHw7w1Zzxuys",
    #"https://www.facebook.com/?feed_demo_ad=120234308362000528&h=AQDVrjrFfQcvyD6y3c8",
    "https://www.facebook.com/100064867445065/videos/2145723119245215",
    "https://www.facebook.com/100064867445065/videos/1167536955203089",
    "https://www.facebook.com/100064867445065/videos/628841873355679",
    "https://www.facebook.com/100064867445065/videos/796131049564901",
    "https://www.facebook.com/100064867445065/videos/2341765776277589",
    "https://www.facebook.com/100064867445065/videos/801738662198924",
    "https://www.facebook.com/100064867445065/videos/755147344070643",
    "https://www.facebook.com/100064867445065/videos/755847064028606",
    "https://www.facebook.com/100064867445065/videos/783931847511846",
    "https://www.facebook.com/100064867445065/videos/1341628570637720",
    "https://www.facebook.com/100064867445065/videos/789273280272516",
    "https://www.facebook.com/100064867445065/videos/814244691266244",
    "https://www.facebook.com/100064867445065/videos/786596180415875",
    "https://www.facebook.com/100064867445065/videos/2234667103680178",
    "https://www.facebook.com/100064867445065/videos/771357842442287",
    "https://www.facebook.com/100064867445065/videos/717835787956854",
    #"https://www.facebook.com/?feed_demo_ad=120233995048100528&h=AQAcR6tkLLA_11gHjYM",
    #"https://www.facebook.com/?feed_demo_ad=120233995047990528&h=AQCB37gks2dqFBAThS8",
    #"https://www.facebook.com/?feed_demo_ad=120233995048070528&h=AQCoaVWJRP7e3fsJAIo",
    #"https://www.facebook.com/?feed_demo_ad=120233993986720528&h=AQBVDcBKhoDww_RVlr4",
    #"https://www.facebook.com/?feed_demo_ad=120233995048130528&h=AQDrNPLcwGAGRGPsHas",
    #"https://www.facebook.com/?feed_demo_ad=120233993986730528&h=AQDDxgkUkY_X66-DEac",
    #"https://www.facebook.com/?feed_demo_ad=120233994998940528&h=AQDJs9D-atUpub4oPCg",
    #"https://www.facebook.com/?feed_demo_ad=120234668517270528&h=AQCA-XaYVDsc9izy908",
    #"https://www.facebook.com/?feed_demo_ad=120234668516960528&h=AQA3o7nPW8R7jkMuWAo",
    #"https://www.facebook.com/?feed_demo_ad=120234668516970528&h=AQBMZUzcJv38cVPqozA",
    #"https://www.facebook.com/?feed_demo_ad=120234668517060528&h=AQBHP3w_eXArpk7xjqY",
    #"https://www.facebook.com/?feed_demo_ad=120234668517160528&h=AQCAL8wo0djxU1XX23Q",
    #"https://www.facebook.com/?feed_demo_ad=120234668517120528&h=AQDmJUDmRujh30MdApA",
    #"https://www.facebook.com/?feed_demo_ad=120234668517210528&h=AQD_qAo0gtb_oV_cNLI",
    "https://www.facebook.com/100064867445065/videos/1263741155491594",
    "https://www.facebook.com/100064867445065/videos/4052016791717647",
    "https://www.facebook.com/100064867445065/videos/1939230727004201",
    "https://www.facebook.com/100064867445065/videos/561032720370911",
    "https://www.facebook.com/100064867445065/videos/1306123300908239",
    "https://www.facebook.com/100064867445065/videos/650898810937630",
    "https://www.facebook.com/100064867445065/videos/25142626068674397",
    "https://www.facebook.com/100064867445065/videos/781719031113522",
    "https://www.facebook.com/100064867445065/videos/1377632307302350",
    "https://www.facebook.com/100064867445065/videos/1450312649554599",
    "https://www.facebook.com/100064867445065/videos/1112926276913003",
    "https://www.facebook.com/100064867445065/videos/1197451092404401",
    "https://www.facebook.com/100064867445065/videos/785935173895742",
    "https://www.facebook.com/100064867445065/videos/1272941960983516",
    "https://www.facebook.com/100064867445065/videos/1088181969962745",
    "https://www.facebook.com/100064867445065/videos/1478389826688793",
    "https://www.facebook.com/100064867445065/videos/769549319319000",
    "https://www.facebook.com/100064867445065/videos/1856535798542600",
    "https://www.facebook.com/100064867445065/videos/780194147747053",
    "https://www.facebook.com/100064867445065/videos/762541969866656",
    "https://www.facebook.com/100064867445065/videos/2536452253381951",
    "https://www.facebook.com/100064867445065/videos/798142679303135",
    "https://www.facebook.com/100064867445065/videos/764814619498990",
    "https://www.facebook.com/100064867445065/videos/1504052630621665",
    "https://www.facebook.com/100064867445065/videos/812103241156564",
    "https://www.facebook.com/100064867445065/videos/667415203051455",
    "https://www.facebook.com/100064867445065/videos/1071246621839050",
    "https://www.facebook.com/100064867445065/videos/792626393453208",
    "https://www.instagram.com/p/DN57pB1jERi/",
    "https://www.instagram.com/p/DN5_P5ZjLG3/",
    "https://www.instagram.com/p/DN59teFAGER/",
    "https://www.instagram.com/p/DN59tauAKm2/",
    "https://www.instagram.com/p/DN59wfOjMs2/",
    "https://www.instagram.com/p/DN57o8vjFGo/",
    "https://www.instagram.com/p/DN57pBUgFSK/",
    "https://www.instagram.com/p/DN5_STUFZM-/",
    "https://www.instagram.com/p/DN59tq0j1Ph/",
    "https://www.instagram.com/p/DN59tsyAFZ7/",
    "https://www.instagram.com/p/DN5_PjRjI2R/",
    "https://www.instagram.com/p/DN5_RL7DxSB/",
    "https://www.instagram.com/p/DN5_S8hDx4V/",
    "https://www.instagram.com/p/DN59sU1DANX/",
    "https://www.instagram.com/p/DN5_P4RjDXp/",
    "https://www.instagram.com/p/DN57rvEjAd3/",
    "https://www.instagram.com/p/DN5_Pt-gLSZ/",
    "https://www.instagram.com/p/DN5_P3ljOET/",
    "https://www.instagram.com/p/DN59wdzjdKJ/",
    "https://www.instagram.com/p/DN59xBtjADu/",
    "https://www.instagram.com/p/DN57rWvjPIN/",
    "https://www.instagram.com/p/DN59xRNDHUE/",
    "https://www.instagram.com/p/DN57ru6DMwD/",
    "https://www.instagram.com/p/DN596MUj3av/",
    "https://www.instagram.com/p/DN5_StEAAQh/",
    "https://www.instagram.com/p/DN57ojQAEEg/",
    "https://www.instagram.com/p/DN59wEujyAe/",
    "https://www.instagram.com/p/DN57rD8DEKT/",
    "https://www.instagram.com/p/DN5_RuSgNdP/",
    "https://www.instagram.com/p/DN59wgZjE3H/",
    "https://www.instagram.com/p/DN5_Qt7ALPO/",
    "https://www.instagram.com/p/DN5_QZCgPIS/",
    "https://www.instagram.com/p/DN57qGFj2R1/",
    "https://www.instagram.com/p/DN59yVXgMYH/",
    "https://www.instagram.com/p/DN57rF5EYlO/",
    "https://www.instagram.com/p/DN57qXtDMzI/",
    "https://www.instagram.com/p/DOYjdr3gMwG/",
    "https://www.instagram.com/p/DOYjdvAgIxV/",
    "https://www.instagram.com/p/DOYjda3AET_/",
    "https://www.instagram.com/p/DOYjd5igBtb/",
    "https://www.instagram.com/p/DOYjdsPgNIQ/",
    "https://www.instagram.com/p/DOYjeCiAOsO/",
    "https://www.instagram.com/p/DOYjdt_gLRQ/",
    "https://www.instagram.com/p/DOYjcncgOUZ/",
    "https://www.instagram.com/p/DOYjdLYAJr9/",
    "https://www.instagram.com/p/DOYjcsmgISX/",
    "https://www.instagram.com/p/DOYjcmUALj3/",
    "https://www.instagram.com/p/DOYjczjAM7v/",
    #"https://www.facebook.com/?feed_demo_ad=120233994998930528&h=AQC-7PhOtzXwl4Qe6aI",
    #"https://www.facebook.com/?feed_demo_ad=120233995048030528&h=AQDCfBe-uxELRTQkBvk",
    #"https://www.facebook.com/?feed_demo_ad=120233995048140528&h=AQANKHw0FT_pAZLJ_4A",
    #"https://www.facebook.com/?feed_demo_ad=120233995048180528&h=AQBgvEcJ0eebSovxfjk",
    #"https://www.facebook.com/?feed_demo_ad=120233995048160528&h=AQB7Vn-y_3yI6o9cuqI",
    #"https://www.facebook.com/?feed_demo_ad=120233994697320528&h=AQBTDinI3Flzp_mGUvc",
    #"https://www.facebook.com/?feed_demo_ad=120233991477580528&h=AQD81q9sSSl19QEPpJQ",
    #"https://www.facebook.com/?feed_demo_ad=120233995048000528&h=AQDec0zvTJVRA4_u5y4",
    #"https://www.facebook.com/?feed_demo_ad=120233995048190528&h=AQDO4Oj_JPK8l8iHn6c",
    #"https://www.facebook.com/?feed_demo_ad=120233995048170528&h=AQBJ-BsvuIafUeEyzCo",
    #"https://www.facebook.com/?feed_demo_ad=120233995048020528&h=AQDqBcoLcDqWTs5Fgbo",
    #"https://www.facebook.com/?feed_demo_ad=120233995047970528&h=AQBsQTvQuLyZizKY6hU",
    #"https://www.facebook.com/?feed_demo_ad=120233995047960528&h=AQDC7mgGZIZVoJZe6dM",
    #"https://www.facebook.com/?feed_demo_ad=120233995048110528&h=AQAYHS1nlmFyQ4tt_jg",
    #"https://www.facebook.com/?feed_demo_ad=120233995047980528&h=AQDDu3OgYX8L1UUIB-s",
    #"https://www.facebook.com/?feed_demo_ad=120233993986740528&h=AQDCDH4U-eiQ8kFZIWI",
    #"https://www.facebook.com/?feed_demo_ad=120233995048010528&h=AQBE9ixiTV06S32J9Ws",
    #"https://www.facebook.com/?feed_demo_ad=120233995048060528&h=AQB7U_7NMDrH-UvcQMk",
    #"https://www.facebook.com/?feed_demo_ad=120233995048150528&h=AQABjmAFxjAoZh5EwVw",
    #"https://www.facebook.com/?feed_demo_ad=120233995048090528&h=AQAX1rpT91itUflEE1M",
    #"https://www.facebook.com/?feed_demo_ad=120233994823750528&h=AQCG2VveihnRp1MxRzc",
    #"https://www.facebook.com/?feed_demo_ad=120233995048120528&h=AQBLBHsPU_StePdyVVA",
    #"https://www.facebook.com/?feed_demo_ad=120233994823760528&h=AQAxlKtBvA-J3WqhKYE",
    #"https://www.facebook.com/?feed_demo_ad=120233994928910528&h=AQDUAG6LRrtPx_cM8UI",
    #"https://www.facebook.com/?feed_demo_ad=120233995048050528&h=AQAKb4_0xSrzVgUsVLk",
    #"https://www.facebook.com/?feed_demo_ad=120233994928900528&h=AQBm2LIiexVe9HvkbL0",
    #"https://www.facebook.com/?feed_demo_ad=120233995048080528&h=AQB6EW9fLYEykdC0yqA",
    #"https://www.facebook.com/?feed_demo_ad=120233994705960528&h=AQAAhBuve_7IbbmlArY",
    #"https://www.facebook.com/?feed_demo_ad=120233995048040528&h=AQBj16l8P7NuDRAqygc"
] 


# INFORMACIÓN DE CAMPAÑA
CAMPAIGN_INFO = {
    'campaign_name': 'CAMPAÑA_MANUAL_MULTIPLE',
    'campaign_id': 'MANUAL_002',
    'campaign_mes': 'Septiembre 2025',
    'campaign_marca': 'TU_MARCA',
    'campaign_referencia': 'REF_MANUAL',
    'campaign_objetivo': 'Análisis de Comentarios'
}

class SocialMediaScraper:
    def __init__(self, apify_token):
        self.client = ApifyClient(apify_token)

    def detect_platform(self, url):
        if pd.isna(url) or not url: return None
        url = str(url).lower()
        if any(d in url for d in ['facebook.com', 'fb.com']): return 'facebook'
        if 'instagram.com' in url: return 'instagram'
        if 'tiktok.com' in url: return 'tiktok'
        return None

    def clean_url(self, url):
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text):
        if pd.isna(text) or text == '': return ''
        try:
            text = str(text)
            text = html.unescape(text)
            text = unicodedata.normalize('NFKD', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run):
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 300
        start_time = time.time()
        while True:
            run_status = self.client.run(run["id"]).get()
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            time.sleep(10)

    def scrape_facebook_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            run_input = {"startUrls": [{"url": self.clean_url(url)}], "maxComments": max_comments}
            run = self.client.actor("apify/facebook-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_facebook_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_facebook_comments: {e}")
            return []

    def scrape_instagram_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
            run_input = {"directUrls": [url], "resultsType": "comments", "resultsLimit": max_comments}
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_instagram_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_instagram_comments: {e}")
            return []

    def scrape_tiktok_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            run_input = {"postURLs": [self.clean_url(url)], "maxCommentsPerPost": max_comments}
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} comments found.")
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_tiktok_comments: {e}")
            return []

    def _process_facebook_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['createdTime', 'timestamp', 'publishedTime', 'date', 'createdAt', 'publishedAt']
        for comment in items:
            # <-- CORRECCIÓN: Usando tu bucle for original para máxima compatibilidad
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Facebook', 'author_name': self.fix_encoding(comment.get('authorName')), 'author_url': comment.get('authorUrl'), 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': comment.get('repliesCount', 0), 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['timestamp', 'createdTime', 'publishedAt', 'date', 'createdAt', 'taken_at']
        for item in items:
            comments_list = item.get('comments', [item]) if item.get('comments') is not None else [item]
            for comment in comments_list:
                # <-- CORRECCIÓN: Usando tu bucle for original
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                author = comment.get('ownerUsername', '')
                comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Instagram', 'author_name': self.fix_encoding(author), 'author_url': f"https://instagram.com/{author}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': 0, 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'TikTok', 'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')), 'author_url': f"https://www.tiktok.com/@{author_id}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': comment.get('createTime'), 'likes_count': comment.get('diggCount', 0), 'replies_count': comment.get('replyCommentTotal', 0), 'is_reply': 'replyToId' in comment, 'parent_comment_id': comment.get('replyToId'), 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed

def save_to_excel(df, filename):
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            if 'post_number' in df.columns:
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(Total_Comentarios=('comment_text', 'count'), Total_Likes=('likes_count', 'sum')).reset_index()
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False

def process_datetime_columns(df):
    if 'created_time' not in df.columns: return df
    logger.info("Processing datetime columns...")
    # Intenta convertir todo tipo de formatos (timestamps, ISO, etc.) a un datetime unificado
    df['created_time_processed'] = pd.to_datetime(df['created_time'], errors='coerce', utc=True, unit='s')
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(df.loc[mask, 'created_time'], errors='coerce', utc=True)
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    return df

def run_extraction():
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return

    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    post_counter = 0

    for url in valid_urls:
        post_counter += 1
        platform = scraper.detect_platform(url)
        comments = []
        if platform == 'facebook':
            comments = scraper.scrape_facebook_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'instagram':
            comments = scraper.scrape_instagram_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'tiktok':
            comments = scraper.scrape_tiktok_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        else:
            logger.warning(f"Unknown platform for URL: {url}")
        
        all_comments.extend(comments)
        if not SOLO_PRIMER_POST and post_counter < len(valid_urls):
            logger.info("Pausing for 30 seconds between posts...")
            time.sleep(30)

    if not all_comments:
        logger.warning("No comments were extracted. Process finished.")
        return

    logger.info("--- PROCESSING FINAL RESULTS ---")
    df_comments = pd.DataFrame(all_comments)
    df_comments = process_datetime_columns(df_comments)
    
    final_columns = ['post_number', 'platform', 'campaign_name', 'post_url', 'author_name', 'comment_text', 'created_time_processed', 'fecha_comentario', 'hora_comentario', 'likes_count', 'replies_count', 'is_reply', 'author_url', 'created_time_raw']
    existing_cols = [col for col in final_columns if col in df_comments.columns]
    df_comments = df_comments[existing_cols]

    filename = "Comentarios Campaña.xlsx"
    save_to_excel(df_comments, filename)
    logger.info("--- EXTRACTION PROCESS FINISHED ---")

if __name__ == "__main__":
    run_extraction()
















